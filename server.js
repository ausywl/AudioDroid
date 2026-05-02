process.env.WEB_CONCURRENCY = 1;
const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('Audio Relay Server Running');
});

const wss = new WebSocket.Server({ server });

let sender = null;
let receivers = [];

wss.on('connection', (ws, req) => {
  const role = new URL(req.url, 'http://localhost').searchParams.get('role');
  console.log(`Connected: ${role}, total receivers: ${receivers.length}`);

  if (role === 'sender') {
    sender = ws;
    console.log('Sender connected');

    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        // 音频二进制数据，直接转发
        console.log(`Forwarding ${data.length} bytes to ${receivers.length} receivers`);
        receivers.forEach(r => {
          if (r.readyState === WebSocket.OPEN) {
            r.send(data, { binary: true });
          }
        });
      } else {
        // 文字控制消息
        console.log('Sender text msg:', data.toString());
      }
    });

    ws.on('close', () => {
      sender = null;
      console.log('Sender disconnected');
    });

  } else if (role === 'receiver') {
    receivers.push(ws);
    console.log(`Receiver joined, total: ${receivers.length}`);

    // 通知sender
    if (sender && sender.readyState === WebSocket.OPEN) {
      sender.send(JSON.stringify({ event: 'receiver_joined', count: receivers.length }));
    }

    ws.on('close', () => {
      receivers = receivers.filter(r => r !== ws);
      console.log(`Receiver left, total: ${receivers.length}`);
      if (sender && sender.readyState === WebSocket.OPEN) {
        sender.send(JSON.stringify({ event: 'receiver_left', count: receivers.length }));
      }
    });
  }

  ws.on('error', (err) => console.error('WS error:', err));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server on port ${PORT}`));
