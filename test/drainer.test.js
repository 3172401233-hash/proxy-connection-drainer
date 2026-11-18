'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const http2 = require('node:http2');
const { once } = require('node:events');

const { ConnectionDrainer, CLOSE_REASONS } = require('../src/drainer');

function listen(server) {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
  });
}

function httpRequest({ port, path = '/', agent, headers }) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path, agent, headers }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ res, body: Buffer.concat(chunks).toString() }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end();
  });
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test('new connection race: listener refuses newcomers, idle keep-alive closes immediately', async () => {
  const server = http.createServer((req, res) => res.end('ok'));
  const drainer = new ConnectionDrainer(server, { drainTimeoutMs: 2000 });
  const port = await listen(server);

  const agent = new http.Agent({ keepAlive: true });
  const first = await httpRequest({ port, agent });
  assert.equal(first.body, 'ok');
  assert.equal(drainer.connections.size, 1);

  const closed = [];
  drainer.on('connectionClosed', (info) => closed.push(info));

  const drainPromise = drainer.drain();

  // A brand-new connection racing the drain must never be served: the
  // listener is closed, and anything already accepted is closed as idle.
  await assert.rejects(
    httpRequest({ port, headers: { connection: 'close' } }),
    (err) => ['ECONNREFUSED', 'ECONNRESET', 'EPIPE'].includes(err.code)
  );

  const result = await drainPromise;
  assert.equal(closed.length, 1);
  assert.equal(closed[0].kind, 'http1');
  assert.equal(closed[0].reason, CLOSE_REASONS.IDLE_CLOSE);
  assert.deepEqual(result, closed);
  agent.destroy();
});

test('in-flight request finishes and is answered with Connection: close', async () => {
  let finishResponse;
  const server = http.createServer((req, res) => {
    finishResponse = () => res.end('done');
  });
  const drainer = new ConnectionDrainer(server, { drainTimeoutMs: 2000 });
  const port = await listen(server);

  const requestSeen = once(server, 'request');
  const reqPromise = httpRequest({ port });
  await requestSeen; // request is now in-flight on the server

  const drainPromise = drainer.drain();
  await sleep(20); // drain is in progress while the response is pending
  finishResponse();

  const { res, body } = await reqPromise;
  assert.equal(body, 'done');
  assert.equal(res.headers.connection, 'close');

  const closed = await drainPromise;
  assert.equal(closed.length, 1);
  assert.equal(closed[0].reason, CLOSE_REASONS.COMPLETED);
});

test('streaming response is not truncated by drain', async () => {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'text/plain' });
    let index = 0;
    const timer = setInterval(() => {
      res.write(`chunk-${index}\n`);
      index += 1;
      if (index === 5) {
        clearInterval(timer);
        res.end();
      }
    }, 15);
  });
  const drainer = new ConnectionDrainer(server, { drainTimeoutMs: 2000 });
  const port = await listen(server);

  const chunks = [];
  const done = new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path: '/' }, (res) => {
      res.on('data', (chunk) => {
        chunks.push(chunk.toString());
        if (chunks.length === 1) drainer.drain().then(resolve, reject);
      });
    });
    req.on('error', reject);
    req.end();
  });

  const closed = await done;
  // All chunks arrived even though drain started after the first one.
  assert.equal(chunks.join('').split('\n').filter(Boolean).length, 5);
  assert.equal(closed.length, 1);
  assert.equal(closed[0].reason, CLOSE_REASONS.COMPLETED);
});

test('client disconnect is recorded and never blocks the drain', async () => {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.write('start\n'); // then hang forever
  });
  const drainer = new ConnectionDrainer(server, { drainTimeoutMs: 2000 });
  const port = await listen(server);

  const closedEvent = once(drainer, 'connectionClosed');
  const req = http.request({ host: '127.0.0.1', port, path: '/' }, (res) => {
    res.once('data', () => req.destroy()); // client vanishes mid-response
    res.resume();
  });
  req.on('error', () => {});
  req.end();

  const [info] = await closedEvent;
  assert.equal(info.reason, CLOSE_REASONS.CLIENT_DISCONNECT);

  const closed = await drainer.drain();
  assert.equal(closed.length, 1);
  assert.equal(closed[0].reason, CLOSE_REASONS.CLIENT_DISCONNECT);
});

test('http2: GOAWAY stops new streams while active streams complete', async () => {
  const server = http2.createServer((req, res) => {
    setTimeout(() => res.end('h2-ok'), 50);
  });
  const drainer = new ConnectionDrainer(server, { drainTimeoutMs: 2000 });
  const port = await listen(server);

  const client = http2.connect(`http://127.0.0.1:${port}`);
  client.on('error', () => {});

  const streamSeen = once(server, 'stream');
  const goawaySeen = once(client, 'goaway');

  const stream = client.request({ ':path': '/' });
  let body = '';
  stream.on('data', (chunk) => { body += chunk; });
  const streamEnded = once(stream, 'end');

  await streamSeen; // server has an active stream
  const drainPromise = drainer.drain();

  await goawaySeen; // peer was told to stop opening streams

  // New streams on a GOAWAY'd session are rejected.
  assert.throws(
    () => client.request({ ':path': '/late' }),
    (err) => err.code === 'ERR_HTTP2_GOAWAY_SESSION'
  );

  await streamEnded; // the in-flight stream still completes
  assert.equal(body, 'h2-ok');

  const closed = await drainPromise;
  assert.equal(closed.length, 1);
  assert.equal(closed[0].kind, 'http2');
  assert.equal(closed[0].reason, CLOSE_REASONS.GOAWAY);
  client.close();
});

test('drain timeout force-destroys connections that never finish', async () => {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.write('partial'); // never ends
  });
  const drainer = new ConnectionDrainer(server, { drainTimeoutMs: 100 });
  const port = await listen(server);

  const requestSeen = once(server, 'request');
  const reqPromise = httpRequest({ port });
  await requestSeen;

  const started = Date.now();
  const closed = await drainer.drain();
  assert.ok(Date.now() - started < 2000, 'drain must not hang past the timeout');

  assert.equal(closed.length, 1);
  assert.equal(closed[0].reason, CLOSE_REASONS.DRAIN_TIMEOUT);
  await assert.rejects(reqPromise, (err) => err.code === 'ECONNRESET');
});

test('drain with no connections resolves immediately', async () => {
  const server = http.createServer((req, res) => res.end('ok'));
  const drainer = new ConnectionDrainer(server, { drainTimeoutMs: 100 });
  await listen(server);
  const closed = await drainer.drain();
  assert.deepEqual(closed, []);
});
