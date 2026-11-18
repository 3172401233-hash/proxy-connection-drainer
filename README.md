# proxy-connection-drainer

Node.js 优雅下线（graceful shutdown / connection draining）核心库。
解决代理滚动下线时的两难：直接关闭监听套接字会立刻切断所有长连接；
而等待所有连接自然结束又会被 HTTP/1.1 keep-alive 永久挂住。

## 用法

```js
const http = require('node:http');
const { ConnectionDrainer } = require('./src/drainer');

const server = http.createServer(handler);
const drainer = new ConnectionDrainer(server, { drainTimeoutMs: 10_000 });

drainer.on('connectionClosed', ({ id, kind, remoteAddress, reason }) => {
  console.log(`connection #${id} (${kind}) from ${remoteAddress}: ${reason}`);
});

process.on('SIGTERM', async () => {
  const closed = await drainer.drain(); // 解析为每个连接的关闭原因列表
  process.exit(0);
});
```

同时支持 `http.Server`、`https.Server` 与 `http2` 的
`Http2Server` / `Http2SecureServer`（含 `allowHTTP1` 混合模式）。

## 有序关闭的保证

`drain()` 启动后，严格按以下顺序执行：

1. **停止接收新连接**：立即关闭监听套接字（`server.close()`）。
   之后到达的新连接被拒绝；在 close 竞态窗口内已被 accept 但尚未分发
   请求的连接视为 idle，立即关闭。
2. **HTTP/2 会话发送 GOAWAY**：对端收到后不再创建新流
   （新流请求以 `ERR_HTTP2_GOAWAY_SESSION` 拒绝），已活跃的流继续完成。
3. **idle keep-alive 连接立即关闭**：没有 in-flight 请求的 HTTP/1.1
   空闲长连接立刻 `socket.end()`（优雅 FIN），不等待。
4. **in-flight 请求自然完成**：进行中的请求/流式响应完整发完；
   响应头尚未发送的会补上 `Connection: close` 告知对端不要复用连接，
   该连接上最后一个响应结束后立即关闭。
5. **drain 超时兜底**：`drainTimeoutMs` 后仍未结束的连接被
   `socket.destroy()` / `session.destroy()` 强制取消，drain 不会永久挂住。

`drain()` 返回的 Promise 在所有被跟踪的连接都关闭后解析
（自然结束或超时强制取消），且幂等——重复调用返回同一个 Promise。

## 连接关闭原因

每个连接的关闭都会记录到 `drainer.closedConnections` 并触发
`connectionClosed` 事件，`reason` 取值（`CLOSE_REASONS`）：

| reason | 含义 |
| --- | --- |
| `idle-close` | drain 时处于 idle 的 keep-alive 连接，被立即关闭 |
| `completed` | in-flight 请求全部正常完成后关闭 |
| `client-disconnect` | 客户端中途断开（响应未发完对端已离开） |
| `goaway` | HTTP/2 会话经 GOAWAY 优雅关闭 |
| `drain-timeout` | 超过 `drainTimeoutMs` 仍未完成，被强制销毁 |

## 测试

```sh
node --test
```

覆盖：新连接竞争（drain 期间新连接不被服务）、流式响应不截断、
客户端断开不阻塞 drain、HTTP/2 GOAWAY 与活跃流完成、drain 超时强制取消。
