const WebSocket = require('ws');
const http = require('http');

const SERVICE_TIME_ZONE = 'Australia/Sydney';
const SERVICE_START_MINUTES = 11 * 60 + 30;
const SERVICE_END_MINUTES = 23 * 60;

function getServiceMinutes() {
  const parts = new Intl.DateTimeFormat('en-AU', {
    timeZone: SERVICE_TIME_ZONE,
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(new Date());
  const hour = Number(parts.find((p) => p.type === 'hour').value);
  const minute = Number(parts.find((p) => p.type === 'minute').value);
  return hour * 60 + minute;
}

function isServiceOpen() {
  const minutes = getServiceMinutes();
  return minutes >= SERVICE_START_MINUTES && minutes < SERVICE_END_MINUTES;
}

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end(isServiceOpen() ? 'Audio Relay Server Running' : 'Audio Relay Server Sleeping');
});

const wss = new WebSocket.Server({ server });

const DEFAULT_CHANNEL = 'default';
let senders = new Map();
let receiversByChannel = new Map();

function getChannel(url) {
  return url.searchParams.get('channel') || DEFAULT_CHANNEL;
}

function getReceivers(channel) {
  if (!receiversByChannel.has(channel)) {
    receiversByChannel.set(channel, []);
  }
  return receiversByChannel.get(channel);
}

function notifySender(channel, event, count) {
  const sender = senders.get(channel);
  if (sender && sender.readyState === WebSocket.OPEN) {
    sender.send(JSON.stringify({ event, count }));
  }
}

function getReceiverTargets(channel) {
  const channelReceivers = getReceivers(channel);
  if (channel === DEFAULT_CHANNEL) return channelReceivers;
  return channelReceivers.concat(getReceivers(DEFAULT_CHANNEL));
}

function getOpenReceiverCount(channel) {
  return getReceiverTargets(channel).filter((receiver) => receiver.readyState === WebSocket.OPEN).length;
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const role = url.searchParams.get('role');
  const channel = getChannel(url);

  if (!isServiceOpen()) {
    console.log(`Rejected outside service hours: ${role}/${channel}`);
    ws.close(1000, 'Outside service hours');
    return;
  }

  console.log(`Connected: ${role}/${channel}`);

  if (role === 'sender') {
    senders.set(channel, ws);
    console.log(`Sender connected: ${channel}`);
    if (getOpenReceiverCount(channel) > 0) {
      notifySender(channel, 'receiver_joined', getOpenReceiverCount(channel));
    }

    ws.on('message', (data) => {
      if (Buffer.isBuffer(data)) {
        // 音频数据，只转发给同频道接收端
        getReceiverTargets(channel).forEach(r => {
          if (r.readyState === WebSocket.OPEN) r.send(data);
        });
      } else {
        // 文字消息
        const msg = data.toString();
        console.log('Sender msg:', msg);
      }
    });

    ws.on('close', () => {
      if (senders.get(channel) === ws) {
        senders.delete(channel);
      }
      console.log(`Sender disconnected: ${channel}`);
    });

  } else if (role === 'receiver') {
    const receivers = getReceivers(channel);
    receivers.push(ws);
    console.log(`Receivers ${channel}: ${receivers.length}`);

    // 通知sender有接收端上线
    notifySender(channel, 'receiver_joined', getOpenReceiverCount(channel));
    if (channel === DEFAULT_CHANNEL) {
      senders.forEach((sender, senderChannel) => {
        if (senderChannel !== DEFAULT_CHANNEL && sender.readyState === WebSocket.OPEN) {
          sender.send(JSON.stringify({ event: 'receiver_joined', count: getOpenReceiverCount(senderChannel) }));
        }
      });
    }

    ws.on('close', () => {
      const remaining = getReceivers(channel).filter(r => r !== ws);
      receiversByChannel.set(channel, remaining);
      console.log(`Receivers ${channel}: ${remaining.length}`);
      // 通知sender接收端下线
      notifySender(channel, 'receiver_left', getOpenReceiverCount(channel));
      if (channel === DEFAULT_CHANNEL) {
        senders.forEach((sender, senderChannel) => {
          if (senderChannel !== DEFAULT_CHANNEL && sender.readyState === WebSocket.OPEN) {
            sender.send(JSON.stringify({ event: 'receiver_left', count: getOpenReceiverCount(senderChannel) }));
          }
        });
      }
    });
  }

  ws.on('error', (err) => console.error('WS error:', err));
});

const PORT = process.env.PORT || 3000;
setInterval(() => {
  if (isServiceOpen()) return;

  senders.forEach((sender) => {
    if (sender.readyState === WebSocket.OPEN) {
      sender.close(1000, 'Outside service hours');
    }
  });

  receiversByChannel.forEach((receivers) => {
    receivers.forEach((receiver) => {
      if (receiver.readyState === WebSocket.OPEN) {
        receiver.close(1000, 'Outside service hours');
      }
    });
  });
  senders.clear();
  receiversByChannel.clear();
}, 60 * 1000);

server.listen(PORT, () => console.log(`Server on port ${PORT}`));
