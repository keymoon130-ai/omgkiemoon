// 오메가기문 - 방 관리 서버
// 방 코드 하나 = Durable Object 하나. 서버는 메시지를 중계만 하고,
// 게임 진행은 방장 브라우저가 맡아요.
import { DurableObject } from "cloudflare:workers";

const MAX_PLAYERS = 8;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const m = url.pathname.match(/^\/ws\/([A-Z0-9]{4})$/i);
    if (m) {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("WebSocket 전용 주소예요", { status: 426 });
      }
      const code = m[1].toUpperCase();
      const stub = env.ROOMS.get(env.ROOMS.idFromName(code));
      return stub.fetch(request);
    }
    return env.ASSETS ? env.ASSETS.fetch(request) : new Response("Not found", { status: 404 });
  },
};

function clean(s, max) {
  return String(s || "").replace(/[<>]/g, "").trim().slice(0, max);
}

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // 연결 유지용 ping은 서버를 깨우지 않고 바로 답해요 (요금 절약)
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  sockets() {
    return this.ctx.getWebSockets().filter((ws) => {
      const a = ws.deserializeAttachment();
      return a && !a.closed;
    });
  }

  roster() {
    const list = this.sockets().map((ws) => ws.deserializeAttachment());
    list.sort((a, b) => a.joinedAt - b.joinedAt);
    return {
      players: list.map((a) => ({ id: a.id, name: a.name, color: a.color })),
      hostId: list.length ? list[0].id : null,
    };
  }

  send(ws, obj) {
    try { ws.send(typeof obj === "string" ? obj : JSON.stringify(obj)); } catch (e) {}
  }

  broadcast(obj, except) {
    const s = JSON.stringify(obj);
    for (const ws of this.sockets()) if (ws !== except) this.send(ws, s);
  }

  async fetch(request) {
    const url = new URL(request.url);
    const id = clean(url.searchParams.get("id"), 24) || crypto.randomUUID().slice(0, 8);
    const name = clean(url.searchParams.get("name"), 12) || "이름없음";
    const color = /^#[0-9a-f]{6}$/i.test(url.searchParams.get("color") || "")
      ? url.searchParams.get("color") : "#ff7a59";

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    // 같은 id로 다시 들어오면 예전 연결은 정리
    for (const ws of this.sockets()) {
      const a = ws.deserializeAttachment();
      if (a.id === id) {
        a.closed = true;
        ws.serializeAttachment(a);
        try { ws.close(4000, "replaced"); } catch (e) {}
      }
    }

    if (this.sockets().length >= MAX_PLAYERS) {
      this.ctx.acceptWebSocket(server);
      server.serializeAttachment({ closed: true });
      this.send(server, { k: "full" });
      server.close(4001, "full");
      return new Response(null, { status: 101, webSocket: client });
    }

    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ id, name, color, joinedAt: Date.now() });
    const r = this.roster();
    this.send(server, { k: "welcome", you: id, ...r });
    this.broadcast({ k: "roster", ...r }, server);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    const me = ws.deserializeAttachment();
    if (!me || me.closed) return;
    if (typeof raw !== "string" || raw.length > 16000) return;
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.t === "ping") { this.send(ws, { k: "pong", c: msg.c }); return; }

    if (msg.t === "profile") {
      me.name = clean(msg.name, 12) || me.name;
      if (/^#[0-9a-f]{6}$/i.test(msg.color || "")) me.color = msg.color;
      ws.serializeAttachment(me);
      this.broadcast({ k: "roster", ...this.roster() });
      return;
    }

    const d = msg.d;
    if (!d || typeof d !== "object") return;
    const out = JSON.stringify({ k: "m", f: me.id, d });

    if (msg.t === "b") {
      for (const o of this.sockets()) if (o !== ws) this.send(o, out);
    } else if (msg.t === "to" || msg.t === "h") {
      const r = this.roster();
      const target = msg.t === "h" ? r.hostId : msg.to;
      for (const o of this.sockets()) {
        if (o.deserializeAttachment().id === target) { this.send(o, out); break; }
      }
    }
  }

  async webSocketClose(ws) {
    const a = ws.deserializeAttachment();
    if (a && !a.closed) { a.closed = true; ws.serializeAttachment(a); }
    try { ws.close(1000, "bye"); } catch (e) {}
    this.broadcast({ k: "roster", ...this.roster() });
  }

  async webSocketError(ws) {
    await this.webSocketClose(ws);
  }
}
