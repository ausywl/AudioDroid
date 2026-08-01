'use strict';

const http = require('http');
const WebSocket = require('ws');

const SERVICE_TIME_ZONE = 'Australia/Sydney';
const SERVICE_START_MINUTES = 11 * 60 + 30;
const SERVICE_END_MINUTES = 23 * 60;
const DEFAULT_CHANNEL = 'default';
const MAX_CHANNEL_LENGTH = 32;
const MAX_AUDIO_MESSAGE_BYTES = 64 * 1024;
const MAX_BUFFERED_BYTES = 64 * 1024;
const SLOW_RECEIVER_TIMEOUT_MS = 10 * 1000;
const MAX_CONNECTIONS = 100;
const HEALTH_INTERVAL_MS = 30 * 1000;
const DUPLICATE_LOG_INTERVAL_MS = 10 * 60 * 1000;

const senders = new Map();
const receiversByChannel = new Map();
const receiverHealth = new WeakMap();
const duplicateReceiverLogs = new Map();

function log(message) {
  console.log(`${new Date().toISOString()} ${message}`);
}

function logDuplicateReceiver(channel) {
  const now = Date.now();
  const state = duplicateReceiverLogs.get(channel) || {
    lastLoggedAt: 0,
    suppressed: 0,
  };

  if (now - state.lastLoggedAt >= DUPLICATE_LOG_INTERVAL_MS) {
    const suffix = state.suppressed
      ? ` (${state.suppressed} similar attempts suppressed)`
      : '';
    log(`Rejecting duplicate receiver: ${channel}${suffix}`);
    state.lastLoggedAt = now;
    state.suppressed = 0;
  } else {
    state.suppressed += 1;
  }
  duplicateReceiverLogs.set(channel, state);
}

function getServiceMinutes() {
  const parts = new Intl.DateTimeFormat('en-AU', {
    timeZone: SERVICE_TIME_ZONE,
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(new Date());
  const hour = Number(parts.find((part) => part.type === 'hour').value);
  const minute = Number(parts.find((part) => part.type === 'minute').value);
  return hour * 60 + minute;
}

function isServiceOpen() {
  if (process.env.AUDIODROID_FORCE_SERVICE_OPEN === '1') {
    return true;
  }
  const minutes = getServiceMinutes();
  return minutes >= SERVICE_START_MINUTES && minutes < SERVICE_END_MINUTES;
}

function parseConnection(req) {
  const url = new URL(req.url, 'http://localhost');
  const role = url.searchParams.get('role');
  const channel = url.searchParams.get('channel') || DEFAULT_CHANNEL;
  const validRole = role === 'sender' || role === 'receiver';
  const validChannel =
    channel.length <= MAX_CHANNEL_LENGTH && /^[a-zA-Z0-9_-]+$/.test(channel);
  return { role, channel, valid: validRole && validChannel };
}

function getReceivers(channel) {
  return receiversByChannel.get(channel) || [];
}

function ensureReceivers(channel) {
  if (!receiversByChannel.has(channel)) {
    receiversByChannel.set(channel, []);
  }
  return receiversByChannel.get(channel);
}

function getReceiverTargets(channel) {
  const channelReceivers = getReceivers(channel);
  if (channel === DEFAULT_CHANNEL) {
    return channelReceivers;
  }
  return channelReceivers.concat(getReceivers(DEFAULT_CHANNEL));
}

function getOpenReceiverCount(channel) {
  return getReceiverTargets(channel).filter(
    (receiver) => receiver.readyState === WebSocket.OPEN,
  ).length;
}

function safeSend(ws, payload, options) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return false;
  }
  try {
    ws.send(payload, options);
    return true;
  } catch (error) {
    log(`Send failed: ${error.message}`);
    ws.terminate();
    return false;
  }
}

function notifySender(channel, event) {
  const sender = senders.get(channel);
  if (!sender || sender.readyState !== WebSocket.OPEN) {
    return;
  }
  safeSend(
    sender,
    JSON.stringify({
      event,
      count: getOpenReceiverCount(channel),
    }),
  );
}

function notifyDefaultReceiverChange(event) {
  senders.forEach((sender, senderChannel) => {
    if (
      senderChannel !== DEFAULT_CHANNEL &&
      sender.readyState === WebSocket.OPEN
    ) {
      notifySender(senderChannel, event);
    }
  });
}

function forwardAudio(channel, data) {
  getReceiverTargets(channel).forEach((receiver) => {
    if (receiver.readyState !== WebSocket.OPEN) {
      return;
    }

    const health = receiverHealth.get(receiver) || { slowSince: null };
    receiverHealth.set(receiver, health);

    if (receiver.bufferedAmount > MAX_BUFFERED_BYTES) {
      const now = Date.now();
      health.slowSince ??= now;
      if (now - health.slowSince >= SLOW_RECEIVER_TIMEOUT_MS) {
        log(`Closing slow receiver: ${channel}`);
        receiver.terminate();
      }
      return;
    }

    health.slowSince = null;
    safeSend(receiver, data, { binary: true, compress: false });
  });
}

function removeReceiver(channel, ws) {
  const remaining = getReceivers(channel).filter(
    (receiver) => receiver !== ws,
  );
  if (remaining.length > 0) {
    receiversByChannel.set(channel, remaining);
  } else {
    receiversByChannel.delete(channel);
  }

  log(`Receivers ${channel}: ${remaining.length}`);
  notifySender(channel, 'receiver_left');
  if (channel === DEFAULT_CHANNEL) {
    notifyDefaultReceiverChange('receiver_left');
  }
}

const server = http.createServer((req, res) => {
  const body = isServiceOpen()
    ? 'Audio Relay Server Running'
    : 'Audio Relay Server Sleeping';
  res.writeHead(200, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Cache-Control': 'no-store',
    'Content-Length': Buffer.byteLength(body),
  });
  res.end(body);
});

const wss = new WebSocket.Server({
  server,
  maxPayload: MAX_AUDIO_MESSAGE_BYTES,
  perMessageDeflate: false,
});

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  const { role, channel, valid } = parseConnection(req);

  if (!valid) {
    ws.close(1008, 'Invalid role or channel');
    return;
  }
  if (!isServiceOpen()) {
    log(`Rejected outside service hours: ${role}/${channel}`);
    ws.close(1000, 'Outside service hours');
    return;
  }
  if (wss.clients.size > MAX_CONNECTIONS) {
    log(`Rejected connection limit: ${role}/${channel}`);
    ws.close(1013, 'Server busy');
    return;
  }

  if (role === 'sender') {
    log(`Connected: sender/${channel}`);
    const previousSender = senders.get(channel);
    senders.set(channel, ws);

    if (
      previousSender &&
      previousSender !== ws &&
      previousSender.readyState !== WebSocket.CLOSED
    ) {
      log(`Replacing previous sender: ${channel}`);
      previousSender.close(1012, 'Sender replaced');
    }

    const receiverCount = getOpenReceiverCount(channel);
    if (receiverCount > 0) {
      notifySender(channel, 'receiver_joined');
    }

    ws.on('message', (data, isBinary) => {
      if (senders.get(channel) !== ws) {
        return;
      }
      if (!isBinary) {
        log(`Sender message ${channel}: ${data.toString().slice(0, 200)}`);
        return;
      }
      forwardAudio(channel, data);
    });

    ws.on('close', () => {
      if (senders.get(channel) === ws) {
        senders.delete(channel);
      }
      log(`Sender disconnected: ${channel}`);
    });
  } else {
    const existingReceiver = getReceivers(channel).find(
      (receiver) => receiver.readyState === WebSocket.OPEN,
    );
    if (existingReceiver) {
      logDuplicateReceiver(channel);
      ws.close(1000, 'Receiver already connected');
      return;
    }

    log(`Connected: receiver/${channel}`);
    receiversByChannel.set(channel, [ws]);
    receiverHealth.set(ws, { slowSince: null });
    log(`Receivers ${channel}: 1`);

    notifySender(channel, 'receiver_joined');
    if (channel === DEFAULT_CHANNEL) {
      notifyDefaultReceiverChange('receiver_joined');
    }

    let removed = false;
    ws.on('close', () => {
      if (removed) {
        return;
      }
      removed = true;
      removeReceiver(channel, ws);
    });
  }

  ws.on('error', (error) => {
    log(`WebSocket error ${role}/${channel}: ${error.message}`);
  });
});

const healthInterval = setInterval(() => {
  if (!isServiceOpen()) {
    wss.clients.forEach((ws) => {
      if (
        ws.readyState === WebSocket.OPEN ||
        ws.readyState === WebSocket.CONNECTING
      ) {
        ws.close(1000, 'Outside service hours');
      }
    });
    return;
  }

  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.ping();
      } catch (error) {
        log(`Ping failed: ${error.message}`);
        ws.terminate();
      }
    }
  });
}, HEALTH_INTERVAL_MS);
healthInterval.unref();

let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  log(`Received ${signal}, shutting down`);
  clearInterval(healthInterval);

  wss.clients.forEach((ws) => {
    try {
      ws.close(1001, 'Server shutting down');
    } catch (error) {
      ws.terminate();
    }
  });

  server.close(() => {
    log('Server stopped');
    process.exit(0);
  });

  setTimeout(() => process.exit(1), 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
server.on('error', (error) => {
  log(`HTTP server error: ${error.message}`);
});

const PORT = Number(process.env.PORT) || 3000;
server.listen(PORT, () => log(`Server listening on port ${PORT}`));
