const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('Audio Relay Server Running');
});

const wss = new WebSocket.Server({ server });

const channels = {};

function getChannel(name) {
  if (!channels[name]) {
    channels[name] = { sender: null, receivers: [] };
  }
  return channels[name];
}

wss.on('connection', (ws, req) => {
  const params = new URL(req.url, 'http://localhost').searchParams;
  const role = params.get('role');
  const channelName = params.get('channel') || 'default';
  const ch = getChannel(channelName);

  console.log(`New connection: role=${role} channel=${channelName}`);

  if (role === 'sender') {
    // 断开旧的sender
    if (ch.sender && ch.sender.readyState === WebSocket.OPEN) {
      ch.sender.close();
    }
    ch.sender = ws;
    console.log(`[${channelName}] Sender connected`);

    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        ch.receivers.forEach(r => {
          if (r.readyState === WebSocket.OPEN) {
            r.send(data, { binary: true });
          }
        });
      }
    });

    ws.on('close', () => {
      if (ch.sender === ws) ch.sender = null;
      console.log(`[${channelName}] Sender disconnected`);
    });

  } else if (role === 'receiver') {
    ch.receivers.push(ws);
    console.log(`[${channelName}] Receivers: ${ch.receivers.length}`);

    if (ch.sender && ch.sender.readyState === WebSocket.OPEN) {
      ch.sender.send(JSON.stringify({
        event: 'receiver_joined',
        count: ch.receivers.length
      }));
    }

    ws.on('close', () => {
      ch.receivers = ch.receivers.filter(r => r !== ws);
      console.log(`[${channelName}] Receivers: ${ch.receivers.length}`);
      if (ch.sender && ch.sender.readyState === WebSocket.OPEN) {
        ch.sender.send(JSON.stringify({
          event: 'receiver_left',
          count: ch.receivers.length
        }));
      }
    });
  }

  ws.on('error', (err) => console.error('WS error:', err));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server on port ${PORT}`));
