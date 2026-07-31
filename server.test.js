'use strict';

const assert = require('assert');
const { spawn } = require('child_process');
const path = require('path');
const WebSocket = require('ws');

const TEST_PORT = 31877;
const TEST_URL = `ws://127.0.0.1:${TEST_PORT}`;

function waitForOpen(ws) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('open timeout')), 5000);
    ws.once('open', () => {
      clearTimeout(timeout);
      resolve();
    });
    ws.once('error', reject);
  });
}

function waitForMessage(ws) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('message timeout')), 5000);
    ws.once('message', (data, isBinary) => {
      clearTimeout(timeout);
      resolve({ data, isBinary });
    });
    ws.once('error', reject);
  });
}

function waitForClose(ws) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('close timeout')), 5000);
    ws.once('close', (code) => {
      clearTimeout(timeout);
      resolve(code);
    });
    ws.once('error', reject);
  });
}

function waitForServer(child) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('server timeout')), 5000);
    child.stdout.on('data', (chunk) => {
      if (chunk.toString().includes('Server listening')) {
        clearTimeout(timeout);
        resolve();
      }
    });
    child.once('error', reject);
    child.once('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`server exited with ${code}`));
      }
    });
  });
}

async function run() {
  const server = spawn(process.execPath, ['server.js'], {
    cwd: __dirname,
    env: {
      ...process.env,
      PORT: String(TEST_PORT),
      AUDIODROID_FORCE_SERVICE_OPEN: '1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  const sockets = [];
  try {
    await waitForServer(server);

    const receiver = new WebSocket(`${TEST_URL}?role=receiver&channel=dakang`);
    sockets.push(receiver);
    await waitForOpen(receiver);

    const sender1 = new WebSocket(`${TEST_URL}?role=sender&channel=dakang`);
    sockets.push(sender1);
    await waitForOpen(sender1);
    const joined1 = await waitForMessage(sender1);
    assert.equal(JSON.parse(joined1.data.toString()).count, 1);

    const sender1Closed = waitForClose(sender1);
    const sender2 = new WebSocket(`${TEST_URL}?role=sender&channel=dakang`);
    sockets.push(sender2);
    await waitForOpen(sender2);
    const joined2 = await waitForMessage(sender2);
    assert.equal(JSON.parse(joined2.data.toString()).count, 1);
    assert.equal(await sender1Closed, 1012);

    const receivedAudio = waitForMessage(receiver);
    sender2.send(Buffer.from([1, 2, 3, 4]));
    const audio = await receivedAudio;
    assert.equal(audio.isBinary, true);
    assert.deepEqual([...audio.data], [1, 2, 3, 4]);

    console.log('Server integration test OK');
  } finally {
    sockets.forEach((ws) => {
      try {
        ws.close();
      } catch (error) {
        // Best-effort test cleanup.
      }
    });
    server.kill();
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
