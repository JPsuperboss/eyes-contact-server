// Eyes Contact — serveur de signalisation + appariement aléatoire

import { WebSocketServer } from "./ws-lite.js";
import { randomUUID } from "crypto";
import http from "http";

const PORT = process.env.PORT || 8787;

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, waiting: queue.length, connected: clients.size }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server });

const clients = new Map();
const queue = [];
const reports = [];

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function removeFromQueue(ws) {
  const idx = queue.indexOf(ws);
  if (idx !== -1) queue.splice(idx, 1);
}

function tryMatch() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();
    const aState = clients.get(a);
    const bState = clients.get(b);
    if (!aState || !bState) continue;
    aState.state = "matched";
    bState.state = "matched";
    aState.peer = b;
    bState.peer = a;
    send(a, { type: "matched", initiator: true, peerId: bState.id });
    send(b, { type: "matched", initiator: false, peerId: aState.id });
  }
}

function requeue(ws) {
  const state = clients.get(ws);
  if (!state) return;
  state.state = "queued";
  state.peer = null;
  queue.push(ws);
  send(ws, { type: "queued" });
  tryMatch();
}

function handleLeavePeer(ws, reason) {
  const state = clients.get(ws);
  if (!state || !state.peer) return;
  const peerWs = state.peer;
  const peerState = clients.get(peerWs);
  state.peer = null;
  if (peerState) {
    peerState.peer = null;
    peerState.state = "idle";
    send(peerWs, { type: "peer-left", reason });
  }
}

wss.on("connection", (ws) => {
  const id = randomUUID();
  clients.set(ws, { id, state: "idle", peer: null, ageConfirmed: false });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    const state = clients.get(ws);
    if (!state) return;

    switch (msg.type) {
      case "join": {
        if (!msg.ageConfirmed) {
          send(ws, { type: "error", message: "age-confirmation-required" });
          ws.close(4001, "age-confirmation-required");
          return;
        }
        state.ageConfirmed = true;
        if (state.state === "idle") requeue(ws);
        break;
      }
      case "signal": {
        if (state.state === "matched" && state.peer) {
          send(state.peer, { type: "signal", data: msg.data });
        }
        break;
      }
      case "next": {
        handleLeavePeer(ws, "skipped");
        requeue(ws);
        break;
      }
      case "report": {
        const peerWs = state.peer;
        const peerState = peerWs ? clients.get(peerWs) : null;
        reports.push({
          ts: Date.now(),
          reporterId: state.id,
          reportedId: peerState ? peerState.id : null,
          reason: typeof msg.reason === "string" ? msg.reason.slice(0, 300) : null,
        });
        console.log(`[report] ${state.id} -> ${peerState ? peerState.id : "unknown"}: ${msg.reason || ""}`);
        if (peerWs && peerState) {
          send(peerWs, { type: "reported" });
          peerWs.close(4002, "reported");
        }
        handleLeavePeer(ws, "reported");
        requeue(ws);
        break;
      }
      default:
        break;
    }
  });

  ws.on("close", () => {
    removeFromQueue(ws);
    handleLeavePeer(ws, "disconnected");
    clients.delete(ws);
  });
});

server.listen(PORT, () => {
  console.log(`Eyes Contact signaling server listening on :${PORT}`);
});
