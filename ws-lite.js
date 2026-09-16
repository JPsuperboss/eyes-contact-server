// ws-lite.js — implémentation minimale d'un serveur WebSocket (RFC 6455)

import { createHash, randomBytes } from "crypto";
import { EventEmitter } from "events";

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const OPCODE = {
  CONTINUATION: 0x0,
  TEXT: 0x1,
  BINARY: 0x2,
  CLOSE: 0x8,
  PING: 0x9,
  PONG: 0xa,
};

function acceptKeyFor(clientKey) {
  return createHash("sha1").update(clientKey + GUID).digest("base64");
}

function encodeFrame(opcode, payload) {
  const payloadLen = payload.length;
  let header;
  if (payloadLen < 126) {
    header = Buffer.alloc(2);
    header[1] = payloadLen;
  } else if (payloadLen < 65536) {
    header = Buffer.alloc(4);
    header[1] = 126;
    header.writeUInt16BE(payloadLen, 2);
  } else {
    header = Buffer.alloc(10);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(payloadLen), 2);
  }
  header[0] = 0x80 | opcode;
  return Buffer.concat([header, payload]);
}

class WSConnection extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.readyState = WSConnection.OPEN;
    this._buffer = Buffer.alloc(0);
    this._fragments = [];
    this._fragmentOpcode = null;

    socket.on("data", (chunk) => this._onData(chunk));
    socket.on("close", () => this._onSocketClose());
    socket.on("error", () => this._onSocketClose());
  }

  _onData(chunk) {
    this._buffer = Buffer.concat([this._buffer, chunk]);
    while (true) {
      if (this._buffer.length < 2) return;
      const first = this._buffer[0];
      const second = this._buffer[1];
      const fin = (first & 0x80) !== 0;
      const opcode = first & 0x0f;
      const masked = (second & 0x80) !== 0;
      let payloadLen = second & 0x7f;
      let offset = 2;

      if (payloadLen === 126) {
        if (this._buffer.length < offset + 2) return;
        payloadLen = this._buffer.readUInt16BE(offset);
        offset += 2;
      } else if (payloadLen === 127) {
        if (this._buffer.length < offset + 8) return;
        payloadLen = Number(this._buffer.readBigUInt64BE(offset));
        offset += 8;
      }

      let maskKey = null;
      if (masked) {
        if (this._buffer.length < offset + 4) return;
        maskKey = this._buffer.subarray(offset, offset + 4);
        offset += 4;
      }

      if (this._buffer.length < offset + payloadLen) return;

      let payload = this._buffer.subarray(offset, offset + payloadLen);
      if (masked) {
        const unmasked = Buffer.alloc(payloadLen);
        for (let i = 0; i < payloadLen; i++) {
          unmasked[i] = payload[i] ^ maskKey[i % 4];
        }
        payload = unmasked;
      }

      this._buffer = this._buffer.subarray(offset + payloadLen);
      this._handleFrame(fin, opcode, payload);
    }
  }

  _handleFrame(fin, opcode, payload) {
    if (opcode === OPCODE.CLOSE) {
      this._sendRaw(OPCODE.CLOSE, Buffer.alloc(0));
      this.socket.end();
      return;
    }
    if (opcode === OPCODE.PING) {
      this._sendRaw(OPCODE.PONG, payload);
      return;
    }
    if (opcode === OPCODE.PONG) {
      return;
    }

    if (opcode === OPCODE.TEXT || opcode === OPCODE.BINARY) {
      this._fragmentOpcode = opcode;
      this._fragments = [payload];
    } else if (opcode === OPCODE.CONTINUATION) {
      this._fragments.push(payload);
    }

    if (fin) {
      const full = Buffer.concat(this._fragments);
      this._fragments = [];
      if (this._fragmentOpcode === OPCODE.TEXT) {
        this.emit("message", full.toString("utf8"));
      }
      this._fragmentOpcode = null;
    }
  }

  _sendRaw(opcode, payload) {
    if (this.socket.destroyed) return;
    this.socket.write(encodeFrame(opcode, payload));
  }

  send(data) {
    if (this.readyState !== WSConnection.OPEN) return;
    this._sendRaw(OPCODE.TEXT, Buffer.from(data, "utf8"));
  }

  close(code = 1000, reason = "") {
    if (this.readyState !== WSConnection.OPEN) return;
    this.readyState = WSConnection.CLOSING;
    const reasonBuf = Buffer.from(reason, "utf8");
    const payload = Buffer.alloc(2 + reasonBuf.length);
    payload.writeUInt16BE(code, 0);
    reasonBuf.copy(payload, 2);
    this._sendRaw(OPCODE.CLOSE, payload);
    this.socket.end();
  }

  _onSocketClose() {
    if (this.readyState === WSConnection.CLOSED) return;
    this.readyState = WSConnection.CLOSED;
    this.emit("close");
  }
}
WSConnection.OPEN = "open";
WSConnection.CLOSING = "closing";
WSConnection.CLOSED = "closed";
WSConnection.prototype.OPEN = WSConnection.OPEN;
WSConnection.prototype.CLOSING = WSConnection.CLOSING;
WSConnection.prototype.CLOSED = WSConnection.CLOSED;

export class WebSocketServer extends EventEmitter {
  constructor({ server }) {
    super();
    this.OPEN = WSConnection.OPEN;
    server.on("upgrade", (req, socket) => this._onUpgrade(req, socket));
  }

  _onUpgrade(req, socket) {
    const key = req.headers["sec-websocket-key"];
    const upgradeHeader = (req.headers["upgrade"] || "").toLowerCase();
    if (upgradeHeader !== "websocket" || !key) {
      socket.destroy();
      return;
    }
    const accept = acceptKeyFor(key);
    const responseHeaders = [
      "HTTP/1.1 101 Switching Protocols",
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Accept: ${accept}`,
      "",
      "",
    ].join("\r\n");
    socket.write(responseHeaders);

    const conn = new WSConnection(socket);
    this.emit("connection", conn, req);
  }
}

export function generateAcceptKeyForTest(key) {
  return acceptKeyFor(key);
}
export const _internal = { randomBytes };
