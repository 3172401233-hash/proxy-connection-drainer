'use strict';

const { EventEmitter } = require('node:events');
const https = require('node:https');

const CLOSE_REASONS = Object.freeze({
  IDLE_CLOSE: 'idle-close',
  COMPLETED: 'completed',
  CLIENT_DISCONNECT: 'client-disconnect',
  DRAIN_TIMEOUT: 'drain-timeout',
  GOAWAY: 'goaway',
});

const noop = () => {};

/**
 * Graceful shutdown coordinator for HTTP/1.1 (keep-alive) and HTTP/2 servers.
 *
 * Drain order:
 *   1. Stop accepting new connections (listener closes).
 *   2. HTTP/2 sessions receive GOAWAY; new streams are rejected by the peer.
 *   3. Idle HTTP/1 keep-alive sockets are closed immediately.
 *   4. In-flight requests / active streams are allowed to finish; their
 *      connections close right after the last response completes.
 *   5. Anything still open after `drainTimeoutMs` is force-destroyed.
 *
 * Every connection close is recorded with a reason (see CLOSE_REASONS) in
 * `closedConnections` and emitted via the `connectionClosed` event.
 */
class ConnectionDrainer extends EventEmitter {
  /**
   * @param {import('node:http').Server|import('node:http2').Http2Server} server
   * @param {{ drainTimeoutMs?: number }} [options]
   */
  constructor(server, options = {}) {
    super();
    if (!server || typeof server.close !== 'function' || typeof server.on !== 'function') {
      throw new TypeError('server must be an http/https/http2 Server instance');
    }

    this.server = server;
    this.drainTimeoutMs = options.drainTimeoutMs ?? 30_000;

    /** @type {Map<import('node:net').Socket, object>} HTTP/1.1 connections by socket */
    this.connections = new Map();
    /** @type {Map<import('node:http2').ServerHttp2Session, object>} HTTP/2 sessions */
    this.sessions = new Map();
    /** @type {Array<{id:number, kind:string, remoteAddress:string|undefined, reason:string}>} */
    this.closedConnections = [];

    this.draining = false;
    this._drainPromise = null;
    this._drainResolve = null;
    this._drainTimer = null;
    this._nextId = 1;

    // node:http2 does not export its server classes; detect by constructor name.
    const ctorName = server.constructor?.name;
    const isHttp2 = ctorName === 'Http2Server' || ctorName === 'Http2SecureServer';
    if (!isHttp2) {
      // For https, req.socket is the TLS socket surfaced by 'secureConnection'.
      const event = server instanceof https.Server ? 'secureConnection' : 'connection';
      server.on(event, (socket) => this._trackHttp1(socket));
    }
    // 'request' covers HTTP/1.1 (and HTTP/2 servers with allowHTTP1).
    server.on('request', (req, res) => this._onRequest(req, res));
    server.on('session', (session) => this._trackSession(session));
    server.on('stream', (stream) => this._onStream(stream));
  }

  /**
   * Begin draining. Idempotent: subsequent calls return the same promise.
   * Resolves with the list of closed-connection records once every tracked
   * connection is gone (naturally, or force-destroyed at the drain timeout).
   */
  drain() {
    if (this._drainPromise) return this._drainPromise;
    this.draining = true;
    this._drainPromise = new Promise((resolve) => {
      this._drainResolve = resolve;
    });

    // Step 1: stop accepting new connections. Already-accepted sockets that
    // surface after this point are treated as idle and closed immediately.
    if (this.server.listening) {
      this.server.close();
    }

    // Step 2: arm the hard deadline for the whole drain.
    if (Number.isFinite(this.drainTimeoutMs)) {
      this._drainTimer = setTimeout(() => this._onDrainTimeout(), this.drainTimeoutMs);
      this._drainTimer.unref?.();
    }

    // Step 3: idle keep-alive sockets go away right now; busy ones close
    // from the response 'finish' handler once their last request completes.
    for (const conn of this.connections.values()) {
      if (conn.inFlight === 0) {
        this._closeHttp1(conn, CLOSE_REASONS.IDLE_CLOSE);
      } else {
        // In-flight responses must tell the peer not to reuse the connection.
        for (const res of conn.pendingResponses) {
          this._markResponseClosing(res);
        }
      }
    }

    // Step 4: GOAWAY every HTTP/2 session; empty sessions close immediately.
    for (const conn of this.sessions.values()) {
      this._shutdownSession(conn);
    }

    this._checkDrained();
    return this._drainPromise;
  }

  // ---------------------------------------------------------------- HTTP/1.1

  _trackHttp1(socket) {
    let conn = this.connections.get(socket);
    if (conn) return conn;
    conn = {
      id: this._nextId++,
      kind: 'http1',
      socket,
      remoteAddress: socket.remoteAddress,
      inFlight: 0,
      pendingResponses: new Set(),
      reason: null,
    };
    this.connections.set(socket, conn);
    socket.on('error', noop); // 'close' always follows; avoid unhandled error
    socket.once('close', () => this._onHttp1Close(conn));
    // A connection that races with drain() is idle by definition (no request
    // could have been dispatched to it yet), so close it immediately.
    if (this.draining) {
      this._closeHttp1(conn, CLOSE_REASONS.IDLE_CLOSE);
    }
    return conn;
  }

  _onRequest(req, res) {
    if (req.stream) return; // HTTP/2 request; tracked via the 'stream' event
    // Lazy tracking also covers allowHTTP1 requests on HTTP/2 servers.
    const conn = this._trackHttp1(req.socket);
    conn.inFlight += 1;
    conn.pendingResponses.add(res);
    if (this.draining) {
      // Tell the peer not to reuse this connection.
      this._markResponseClosing(res);
    }
    let settled = false;
    const settle = () => {
      if (settled) return;
      settled = true;
      conn.inFlight -= 1;
      conn.pendingResponses.delete(res);
      if (!this.connections.has(conn.socket)) return;
      if (!res.writableFinished) {
        // Response never completed: the client went away mid-request.
        this._closeHttp1(conn, CLOSE_REASONS.CLIENT_DISCONNECT);
      } else if (this.draining && conn.inFlight === 0) {
        this._closeHttp1(conn, CLOSE_REASONS.COMPLETED);
      }
    };
    res.once('finish', settle);
    res.once('close', settle);
  }

  _markResponseClosing(res) {
    if (!res.headersSent) {
      res.setHeader('Connection', 'close');
    }
    res.shouldKeepAlive = false;
  }

  _closeHttp1(conn, reason) {
    if (conn.reason) return; // already closing
    conn.reason = reason;
    const { socket } = conn;
    if (socket.destroyed) return;
    if (socket.writable) {
      socket.end(); // graceful FIN; peer may still be reading
    } else {
      socket.destroy();
    }
  }

  _onHttp1Close(conn) {
    if (!this.connections.delete(conn.socket)) return;
    if (!conn.reason) conn.reason = CLOSE_REASONS.CLIENT_DISCONNECT;
    this._recordClosed(conn);
  }

  // ------------------------------------------------------------------ HTTP/2

  _trackSession(session) {
    if (this.sessions.has(session)) return;
    const conn = {
      id: this._nextId++,
      kind: 'http2',
      session,
      remoteAddress: session.socket?.remoteAddress,
      activeStreams: 0,
      reason: null,
    };
    this.sessions.set(session, conn);
    session.on('error', noop);
    session.once('close', () => this._onSessionClose(conn));
    if (this.draining) {
      this._shutdownSession(conn);
    }
  }

  _onStream(stream) {
    const conn = this.sessions.get(stream.session);
    if (!conn) return;
    conn.activeStreams += 1;
    stream.once('close', () => {
      conn.activeStreams -= 1;
      if (this.draining && conn.activeStreams === 0 && this.sessions.has(stream.session)) {
        this._closeSession(conn);
      }
    });
  }

  _shutdownSession(conn) {
    if (conn.reason) return;
    conn.reason = CLOSE_REASONS.GOAWAY;
    try {
      conn.session.goaway();
    } catch {
      // Session already gone; its 'close' handler will clean up.
    }
    if (conn.activeStreams === 0) {
      this._closeSession(conn);
    }
  }

  _closeSession(conn) {
    try {
      conn.session.close();
    } catch {
      // Already closed/destroyed.
    }
  }

  _onSessionClose(conn) {
    if (!this.sessions.delete(conn.session)) return;
    if (!conn.reason) conn.reason = CLOSE_REASONS.CLIENT_DISCONNECT;
    this._recordClosed(conn);
  }

  // ------------------------------------------------------------------ shared

  _onDrainTimeout() {
    for (const conn of this.connections.values()) {
      conn.reason = CLOSE_REASONS.DRAIN_TIMEOUT;
      conn.socket.destroy();
    }
    for (const conn of this.sessions.values()) {
      conn.reason = CLOSE_REASONS.DRAIN_TIMEOUT;
      conn.session.destroy();
    }
  }

  _recordClosed(conn) {
    const info = {
      id: conn.id,
      kind: conn.kind,
      remoteAddress: conn.remoteAddress,
      reason: conn.reason,
    };
    this.closedConnections.push(info);
    this.emit('connectionClosed', info);
    this._checkDrained();
  }

  _checkDrained() {
    if (!this.draining) return;
    if (this.connections.size !== 0 || this.sessions.size !== 0) return;
    if (this._drainTimer) {
      clearTimeout(this._drainTimer);
      this._drainTimer = null;
    }
    const resolve = this._drainResolve;
    if (resolve) {
      this._drainResolve = null;
      resolve(this.closedConnections.slice());
    }
  }
}

module.exports = { ConnectionDrainer, CLOSE_REASONS };
