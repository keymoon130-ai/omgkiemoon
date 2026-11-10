var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// .wrangler/tmp/bundle-ZTtYIX/strip-cf-connecting-ip-header.js
function stripCfConnectingIPHeader(input, init) {
  const request = new Request(input, init);
  request.headers.delete("CF-Connecting-IP");
  return request;
}
__name(stripCfConnectingIPHeader, "stripCfConnectingIPHeader");
globalThis.fetch = new Proxy(globalThis.fetch, {
  apply(target, thisArg, argArray) {
    return Reflect.apply(target, thisArg, [
      stripCfConnectingIPHeader.apply(null, argArray)
    ]);
  }
});

// src/worker.js
import { DurableObject } from "cloudflare:workers";
var MAX_PLAYERS = 8;
var worker_default = {
  async fetch(request, env) {
    const url = new URL(request.url);
    const m = url.pathname.match(/^\/ws\/([A-Z0-9]{4})$/i);
    if (m) {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("WebSocket \uC804\uC6A9 \uC8FC\uC18C\uC608\uC694", { status: 426 });
      }
      const code = m[1].toUpperCase();
      const stub = env.ROOMS.get(env.ROOMS.idFromName(code));
      return stub.fetch(request);
    }
    return env.ASSETS ? env.ASSETS.fetch(request) : new Response("Not found", { status: 404 });
  }
};
function clean(s, max) {
  return String(s || "").replace(/[<>]/g, "").trim().slice(0, max);
}
__name(clean, "clean");
var Room = class extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
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
      hostId: list.length ? list[0].id : null
    };
  }
  send(ws, obj) {
    try {
      ws.send(typeof obj === "string" ? obj : JSON.stringify(obj));
    } catch (e) {
    }
  }
  broadcast(obj, except) {
    const s = JSON.stringify(obj);
    for (const ws of this.sockets())
      if (ws !== except)
        this.send(ws, s);
  }
  async fetch(request) {
    const url = new URL(request.url);
    const id = clean(url.searchParams.get("id"), 24) || crypto.randomUUID().slice(0, 8);
    const name = clean(url.searchParams.get("name"), 12) || "\uC774\uB984\uC5C6\uC74C";
    const color = /^#[0-9a-f]{6}$/i.test(url.searchParams.get("color") || "") ? url.searchParams.get("color") : "#ff7a59";
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    for (const ws of this.sockets()) {
      const a = ws.deserializeAttachment();
      if (a.id === id) {
        a.closed = true;
        ws.serializeAttachment(a);
        try {
          ws.close(4e3, "replaced");
        } catch (e) {
        }
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
    if (!me || me.closed)
      return;
    if (typeof raw !== "string" || raw.length > 16e3)
      return;
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      return;
    }
    if (msg.t === "ping") {
      this.send(ws, { k: "pong", c: msg.c });
      return;
    }
    if (msg.t === "profile") {
      me.name = clean(msg.name, 12) || me.name;
      if (/^#[0-9a-f]{6}$/i.test(msg.color || ""))
        me.color = msg.color;
      ws.serializeAttachment(me);
      this.broadcast({ k: "roster", ...this.roster() });
      return;
    }
    const d = msg.d;
    if (!d || typeof d !== "object")
      return;
    const out = JSON.stringify({ k: "m", f: me.id, d });
    if (msg.t === "b") {
      for (const o of this.sockets())
        if (o !== ws)
          this.send(o, out);
    } else if (msg.t === "to" || msg.t === "h") {
      const r = this.roster();
      const target = msg.t === "h" ? r.hostId : msg.to;
      for (const o of this.sockets()) {
        if (o.deserializeAttachment().id === target) {
          this.send(o, out);
          break;
        }
      }
    }
  }
  async webSocketClose(ws) {
    const a = ws.deserializeAttachment();
    if (a && !a.closed) {
      a.closed = true;
      ws.serializeAttachment(a);
    }
    try {
      ws.close(1e3, "bye");
    } catch (e) {
    }
    this.broadcast({ k: "roster", ...this.roster() });
  }
  async webSocketError(ws) {
    await this.webSocketClose(ws);
  }
};
__name(Room, "Room");

// ../../../tmp/claude-0/-home-claude/11bb6d18-ca5b-57a1-9f45-529d2afe2b2d/scratchpad/test/node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// ../../../tmp/claude-0/-home-claude/11bb6d18-ca5b-57a1-9f45-529d2afe2b2d/scratchpad/test/node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } catch (e) {
    const error = reduceError(e);
    return Response.json(error, {
      status: 500,
      headers: { "MF-Experimental-Error-Stack": "true" }
    });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-ZTtYIX/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = worker_default;

// ../../../tmp/claude-0/-home-claude/11bb6d18-ca5b-57a1-9f45-529d2afe2b2d/scratchpad/test/node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-ZTtYIX/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof __Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
__name(__Facade_ScheduledController__, "__Facade_ScheduledController__");
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = (request, env, ctx) => {
      this.env = env;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    };
    #dispatcher = (type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    };
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  Room,
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=worker.js.map
