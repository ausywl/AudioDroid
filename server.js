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
let conflictTimer = null;

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

function getOpenReceiverCount(channel) {
  return getReceivers(channel).filter((r) => r.readyState === WebSocket.OPEN).length;
}

function checkConflict() {
  const activeChannels = [];
  receiversByChannel.forEach((receivers, channel) => {
    if (channel !== DEFAULT_CHANNEL) {
      const count = receivers.filter(r => r.readyState === WebSocket.OPEN).length;
      if (count > 0) activeChannels.push(channel);
    }
  });

  if (activeChannels.length > 1) {
    console.log(`Conflict detected: ${activeChannels.join(', ')} - stopping all`);
    activeChannels.forEach(channel => {
      notifySender(channel, 'conflict', 0);
    });
  }
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
    if (senders.has(channel)) {
      const old = senders.get(channel);
      if (old.readyState === WebSocket.OPEN) old.close();
    }
    senders.set(channel, ws);
    console.log(`Sender connected: ${channel}`);

    if (getOpenReceiverCount(channel) > 0) {
      notifySender(channel, 'receiver_joined', getOpenReceiverCount(channel));
    }

    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        getReceivers(channel).forEach(r => {
          if (r.readyState === WebSocket.OPEN) r.send(data, { binary: true });
        });
      }
    });

    ws.on('close', () => {
      if (senders.get(channel) === ws) senders.delete(channel);
      console.log(`Sender disconnected: ${channel}`);
    });

  } else if (role === 'receiver') {
    const receivers = getReceivers(channel);
    receivers.push(ws);
    console.log(`Receivers ${channel}: ${receivers.length}`);

    // 延迟3秒检查冲突
    if (conflictTimer) clearTimeout(conflictTimer);
    conflictTimer = setTimeout(checkConflict, 3000);

    // 通知sender
    notifySender(channel, 'receiver_joined', getOpenReceiverCount(channel));

    ws.on('close', () => {
      const remaining = getReceivers(channel).filter(r => r !== ws);
      receiversByChannel.set(channel, remaining);
      console.log(`Receivers ${channel}: ${remaining.length}`);
      notifySender(channel, 'receiver_left', getOpenReceiverCount(channel));
    });
  }

  ws.on('error', (err) => console.error('WS error:', err));
});

setInterval(() => {
  if (isServiceOpen()) return;
  senders.forEach((sender) => {
    if (sender.readyState === WebSocket.OPEN) sender.close(1000, 'Outside service hours');
  });
  receiversByChannel.forEach((receivers) => {
    receivers.forEach((receiver) => {
      if (receiver.readyState === WebSocket.OPEN) receiver.close(1000, 'Outside service hours');
    });
  });
  senders.clear();
  receiversByChannel.clear();
}, 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server on port ${PORT}`));
