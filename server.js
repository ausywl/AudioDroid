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
  console.log(`Connected: ${role}`);

  if (role === 'sender') {
    sender = ws;
    console.log('Sender connected');

    ws.on('message', (data) => {
      if (Buffer.isBuffer(data)) {
        // 音频数据，转发给所有接收端
        receivers.forEach(r => {
          if (r.readyState === WebSocket.OPEN) r.send(data);
        });
      } else {
        // 文字消息
        const msg = data.toString();
        console.log('Sender msg:', msg);
      }
    });

    ws.on('close', () => {
      sender = null;
      console.log('Sender disconnected');
    });

  } else if (role === 'receiver') {
    receivers.push(ws);
    console.log(`Receivers: ${receivers.length}`);

    // 通知sender有接收端上线
    if (sender && sender.readyState === WebSocket.OPEN) {
      sender.send(JSON.stringify({ event: 'receiver_joined', count: receivers.length }));
    }

    ws.on('close', () => {
      receivers = receivers.filter(r => r !== ws);
      console.log(`Receivers: ${receivers.length}`);
      // 通知sender接收端下线
      if (sender && sender.readyState === WebSocket.OPEN) {
        sender.send(JSON.stringify({ event: 'receiver_left', count: receivers.length }));
      }
    });
  }

  ws.on('error', (err) => console.error('WS error:', err));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server on port ${PORT}`));