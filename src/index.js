/**
 * SACRIFICE ephemeral relay.
 * - Forwards live text/signaling only; it never writes chat messages to storage.
 * - Stores only the shared voice-node list in Durable Object storage.
 * - Voice media itself is peer-to-peer WebRTC, not sent through this Worker.
 */
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return Response.json({
        app: "SACRIFICE",
        status: "ok",
        roomKeyConfigured: Boolean(env.ROOM_KEY),
        durableObjectConfigured: Boolean(env.CHAT_ROOM),
        websocketEndpoint: "/ws",
        mediaBucketConfigured: Boolean(env.MEDIA_BUCKET),
        mediaStorage: "R2 (optional binding MEDIA_BUCKET)",
        messageHistoryStored: false
      });
    }

    // Public media reads use opaque, random object keys. Uploads remain authenticated.
    if (url.pathname.startsWith("/media/")) {
      if (request.method !== "GET" && request.method !== "HEAD") {
        return new Response("Method not allowed", { status: 405, headers: { Allow: "GET, HEAD" } });
      }
      if (!env.MEDIA_BUCKET) return Response.json({ error: "MEDIA_BUCKET_NOT_CONFIGURED" }, { status: 503 });
      const objectKey = url.pathname.slice("/media/".length);
      if (!objectKey.startsWith("uploads/") || objectKey.includes("..")) return new Response("Not found", { status: 404 });
      const object = await env.MEDIA_BUCKET.get(objectKey);
      if (!object) return new Response("Not found", { status: 404 });
      const headers = new Headers();
      object.writeHttpMetadata(headers);
      headers.set("Cache-Control", "public, max-age=31536000, immutable");
      headers.set("X-Content-Type-Options", "nosniff");
      if (!headers.has("Content-Type")) headers.set("Content-Type", "application/octet-stream");
      return new Response(request.method === "HEAD" ? null : object.body, { headers });
    }

    if (url.pathname !== "/ws" && !(url.pathname === "/media" && request.method === "POST")) {
      return new Response("SACRIFICE relay. WebSocket endpoint: /ws", { status: 404 });
    }

    const headerKey = request.headers.get("X-SACRIFICE-KEY") || "";
    const suppliedCandidates = [headerKey];
    // Newer Android builds send Base64URL to keep even Unicode keys header-safe.
    // Older published builds sent the ASCII ROOM_KEY directly. Accept both formats
    // so updating the Android app does not require a perfectly synchronized rollout.
    if (headerKey) {
      try {
        const base64 = headerKey.replace(/-/g, "+").replace(/_/g, "/");
        const binary = atob(base64 + "=".repeat((4 - base64.length % 4) % 4));
        const bytes = Uint8Array.from(binary, character => character.charCodeAt(0));
        suppliedCandidates.push(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
      } catch (_) {
        // A plain ASCII header is valid too; failed Base64 decoding is not fatal.
      }
    }
    const expectedKey = String(env.ROOM_KEY || "").trim();
    const matches = expectedKey && suppliedCandidates.some(value => constantTimeEquals(String(value).trim(), expectedKey));
    if (!matches) {
      return new Response("Unauthorized", { status: 401 });
    }

    if (url.pathname === "/media" && request.method === "POST") {
      if (!env.MEDIA_BUCKET) return Response.json({ error: "MEDIA_BUCKET_NOT_CONFIGURED", message: "Crie o bucket R2 sacrifice-media e configure o binding MEDIA_BUCKET." }, { status: 503 });
      const contentType = (request.headers.get("Content-Type") || "").split(";")[0].trim().toLowerCase();
      const allowedTypes = new Set(["image/jpeg", "image/png", "image/webp", "image/gif", "video/mp4", "video/webm", "video/quicktime"]);
      if (!allowedTypes.has(contentType)) return Response.json({ error: "UNSUPPORTED_MEDIA_TYPE" }, { status: 415 });
      const isVideo = contentType.startsWith("video/");
      const maxBytes = isVideo ? 40 * 1024 * 1024 : 12 * 1024 * 1024;
      const declaredSize = Number(request.headers.get("Content-Length") || 0);
      if (declaredSize <= 0 || declaredSize > maxBytes) return Response.json({ error: "FILE_SIZE_LIMIT", maxBytes }, { status: 413 });
      const bytes = await request.arrayBuffer();
      if (bytes.byteLength <= 0 || bytes.byteLength > maxBytes) return Response.json({ error: "FILE_SIZE_LIMIT", maxBytes }, { status: 413 });
      const extensions = { "image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif", "video/mp4": "mp4", "video/webm": "webm", "video/quicktime": "mov" };
      const objectKey = `uploads/${crypto.randomUUID()}.${extensions[contentType] || "bin"}`;
      let decodedName = request.headers.get("X-Media-Name") || "arquivo";
      try { decodedName = decodeURIComponent(decodedName); } catch { /* Keep the raw name if it is not valid URL encoding. */ }
      const rawName = decodedName.replace(/[\r\n\0]/g, " ").trim().slice(0, 100) || "arquivo";
      await env.MEDIA_BUCKET.put(objectKey, bytes, {
        httpMetadata: { contentType, cacheControl: "public, max-age=31536000, immutable" },
        customMetadata: { originalName: rawName, uploadedAt: new Date().toISOString() }
      });
      return Response.json({ url: `${url.origin}/media/${objectKey}`, mediaType: contentType, name: rawName, size: bytes.byteLength });
    }

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket upgrade required", { status: 426 });
    }

    const id = env.CHAT_ROOM.idFromName("sacrifice-main-room-v1");
    return env.CHAT_ROOM.get(id).fetch(request);
  }
};

export class ChatRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket upgrade required", { status: 426 });
    }
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({
      id: crypto.randomUUID(), name: "", badge: "", avatar: "", avatarUrl: "", status: "", presence: "online", voice: "",
      voiceMuted: false, serverMuted: false, deafened: false
    });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, rawMessage) {
    if (typeof rawMessage !== "string" || rawMessage.length > 65000) {
      this.safeSend(ws, { type: "relay_error", message: "Message too large or invalid." });
      return;
    }

    let data;
    try { data = JSON.parse(rawMessage); }
    catch { this.safeSend(ws, { type: "relay_error", message: "Invalid event format." }); return; }
    if (!data || typeof data.type !== "string") return;

    const attachment = ws.deserializeAttachment() || {
      id: crypto.randomUUID(), name: "", badge: "", avatar: "", avatarUrl: "", status: "", presence: "online", voice: "",
      voiceMuted: false, serverMuted: false, deafened: false
    };
    const ownId = attachment.id;

    if (data.type === "hello") {
      const requestedName = this.clean(data.name, 32).replace(/\s+/g, " ");
      if (!requestedName) {
        try { ws.close(1008, "PROFILE_REQUIRED"); } catch { }
        return;
      }
      attachment.id = this.clean(data.id, 64) || ownId;
      attachment.name = requestedName;
      attachment.badge = this.clean(data.badge, 8);
      attachment.avatar = this.clean(data.avatar, 4);
      attachment.avatarUrl = this.clean(data.avatarUrl, 500);
      attachment.status = this.clean(data.status, 36);
      attachment.presence = this.cleanPresence(data.presence);
      attachment.voice = "";
      attachment.voiceMuted = false;
      attachment.serverMuted = false;
      attachment.deafened = false;
      ws.serializeAttachment(attachment);
      let ownerId = await this.ctx.storage.get("ownerId");
      if (!ownerId) {
        ownerId = attachment.id;
        await this.ctx.storage.put("ownerId", ownerId);
      }
      const channels = (await this.ctx.storage.get("channels")) || [];
      this.safeSend(ws, { type: "welcome", id: attachment.id, ownerId, peers: this.peerSnapshot(ws), channels });
      this.broadcast({ type: "presence_update", peers: this.peerSnapshot() });
      return;
    }

    if (data.type === "profile_update") {
      const name = this.clean(data.name, 32).replace(/\s+/g, " ");
      if (!name) return;
      attachment.name = name;
      attachment.badge = this.clean(data.badge, 8);
      attachment.avatar = this.clean(data.avatar, 4);
      attachment.avatarUrl = this.clean(data.avatarUrl, 500);
      attachment.status = this.clean(data.status, 36);
      attachment.presence = this.cleanPresence(data.presence || attachment.presence);
      ws.serializeAttachment(attachment);
      this.broadcast({ type: "presence_update", peers: this.peerSnapshot() });
      return;
    }

    if (data.type === "channels_update") {
      const channels = this.cleanChannelList(data.channels);
      if (!channels.length) return;
      await this.ctx.storage.put("channels", channels);
      const outgoing = { type: "channels_update", channels };
      const target = this.clean(data.to, 64);
      if (target && target !== "*") this.sendToId(target, outgoing);
      else this.broadcast(outgoing);
      return;
    }

    if (data.type === "channels_request") {
      this.broadcast({ type: "channels_request", from: ownId }, ws);
      return;
    }

    if (data.type === "chat") {
      const text = this.clean(data.text, 2000);
      const media = this.cleanMedia(data);
      if ((!text && !media.url) || !attachment.name) return;
      this.broadcast({
        type: "chat",
        id: this.clean(data.id, 64) || crypto.randomUUID(),
        from: ownId,
        to: "",
        name: attachment.name,
        badge: attachment.badge || "",
        avatar: attachment.avatar || "",
        avatarUrl: attachment.avatarUrl || "",
        time: this.clean(data.time, 10) || new Date().toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", timeZone: "UTC" }),
        text,
        mediaUrl: media.url,
        mediaType: media.type,
        mediaName: media.name,
        at: Number.isFinite(Number(data.at)) ? Number(data.at) : Date.now()
      });
      return;
    }

    if (data.type === "dm") {
      const to = this.clean(data.to, 64);
      const text = this.clean(data.text, 2000);
      const media = this.cleanMedia(data);
      if (!to || to === ownId || (!text && !media.url) || !attachment.name) return;
      const recipient = this.findSocket(to);
      if (!recipient) {
        this.safeSend(ws, { type: "dm_error", to, message: "A pessoa está offline. A mensagem privada ficou salva apenas neste aparelho." });
        return;
      }
      const message = {
        type: "dm",
        id: this.clean(data.id, 64) || crypto.randomUUID(),
        from: ownId,
        to,
        name: attachment.name,
        badge: attachment.badge || "",
        avatar: attachment.avatar || "",
        avatarUrl: attachment.avatarUrl || "",
        time: this.clean(data.time, 10) || new Date().toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", timeZone: "UTC" }),
        text,
        mediaUrl: media.url,
        mediaType: media.type,
        mediaName: media.name,
        at: Number.isFinite(Number(data.at)) ? Number(data.at) : Date.now()
      };
      if (!this.safeSend(recipient, message)) {
        this.safeSend(ws, { type: "dm_error", to, message: "A conexão da pessoa caiu; a mensagem privada não foi entregue." });
        return;
      }
      this.safeSend(ws, { type: "dm_sent", id: message.id, to });
      return;
    }

    if (data.type === "history_request") {
      this.broadcast({ type: "history_request", from: ownId }, ws);
      return;
    }

    if (data.type === "history_bundle") {
      if (!Array.isArray(data.messages)) return;
      const sanitized = [];
      for (const item of data.messages.slice(-180)) {
        if (!item || typeof item !== "object") continue;
        if (this.clean(item.to || item.recipientId, 64)) continue;
        const text = this.clean(item.text, 2000);
        const media = this.cleanMedia(item);
        if (!text && !media.url) continue;
        sanitized.push({
          id: this.clean(item.id, 64) || crypto.randomUUID(),
          time: this.clean(item.time, 10),
          name: this.clean(item.name, 32),
          badge: this.clean(item.badge, 8),
          avatar: this.clean(item.avatar, 4),
          avatarUrl: this.clean(item.avatarUrl, 500),
          from: this.clean(item.from, 64),
          to: "",
          text,
          mediaUrl: media.url,
          mediaType: media.type,
          mediaName: media.name,
          at: Number.isFinite(Number(item.at)) ? Number(item.at) : Date.now()
        });
      }
      const outgoing = { type: "history_bundle", from: ownId, messages: sanitized };
      const packed = JSON.stringify(outgoing);
      if (packed.length > 60000) {
        this.safeSend(ws, { type: "relay_error", message: "History is too large; share fewer messages." });
        return;
      }
      const target = this.clean(data.to, 64);
      if (target && target !== "*") this.sendToId(target, outgoing);
      else this.broadcast(outgoing);
      return;
    }

    if (data.type === "voice_join") {
      const channel = this.clean(data.channel, 28);
      if (!channel) return;
      attachment.voice = channel;
      attachment.voiceMuted = false;
      attachment.serverMuted = false;
      attachment.deafened = false;
      if (!attachment.name) return;
      attachment.badge = this.clean(data.badge, 8);
      attachment.avatar = this.clean(data.avatar, 4);
      attachment.avatarUrl = this.clean(data.avatarUrl, 500);
      attachment.status = this.clean(data.status, 36);
      ws.serializeAttachment(attachment);
      const oldPeers = this.peerSnapshot().filter(peer => peer.id !== ownId && peer.voice === channel);
      for (const peer of oldPeers) this.safeSend(ws, { type: "voice_peer_joined", peer });
      this.broadcast({ type: "voice_peer_joined", peer: { id: ownId, name: attachment.name, voice: channel, badge: attachment.badge || "", avatar: attachment.avatar || "", avatarUrl: attachment.avatarUrl || "" } }, ws);
      this.broadcast({ type: "presence_update", peers: this.peerSnapshot() });
      return;
    }

    if (data.type === "voice_leave") {
      const channel = attachment.voice;
      attachment.voice = "";
      attachment.voiceMuted = false;
      attachment.serverMuted = false;
      attachment.deafened = false;
      ws.serializeAttachment(attachment);
      if (channel) this.broadcast({ type: "voice_peer_left", peer: { id: ownId, name: attachment.name, voice: channel } }, ws);
      this.broadcast({ type: "presence_update", peers: this.peerSnapshot() });
      return;
    }

    if (data.type === "voice_state") {
      if (!attachment.voice) return;
      attachment.voiceMuted = Boolean(data.voiceMuted);
      attachment.deafened = Boolean(data.deafened);
      ws.serializeAttachment(attachment);
      this.broadcast({ type: "presence_update", peers: this.peerSnapshot() });
      return;
    }

    if (data.type === "voice_moderation") {
      const ownerId = await this.ctx.storage.get("ownerId");
      if (!ownerId || ownId !== ownerId) {
        this.safeSend(ws, { type: "relay_error", message: "Only the room owner can manage voice participants." });
        return;
      }
      const targetId = this.clean(data.targetId, 64);
      const action = this.clean(data.action, 24);
      if (!targetId || targetId === ownId) return;
      const targetSocket = this.findSocket(targetId);
      if (!targetSocket) return;
      const targetInfo = targetSocket.deserializeAttachment() || {};
      const oldChannel = this.clean(targetInfo.voice, 28);
      if (!oldChannel) return;

      if (action === "server_mute" || action === "deafen") {
        const enabled = Boolean(data.enabled);
        if (action === "server_mute") {
          targetInfo.serverMuted = enabled;
        } else {
          targetInfo.deafened = enabled;
        }
        targetSocket.serializeAttachment(targetInfo);
        this.safeSend(targetSocket, { type: "voice_moderation", action, enabled, targetId, by: ownId });
        this.broadcast({ type: "presence_update", peers: this.peerSnapshot() });
        return;
      }

      if (action === "disconnect") {
        targetInfo.voice = "";
        targetInfo.voiceMuted = false;
        targetInfo.serverMuted = false;
        targetInfo.deafened = false;
        targetSocket.serializeAttachment(targetInfo);
        this.safeSend(targetSocket, { type: "voice_moderation", action, targetId, oldChannel, by: ownId });
        this.broadcast({ type: "voice_peer_left", peer: { id: targetInfo.id, name: targetInfo.name, voice: oldChannel } });
        this.broadcast({ type: "presence_update", peers: this.peerSnapshot() });
        return;
      }

      if (action === "move") {
        const destination = this.clean(data.destination, 28);
        if (!destination || destination.toLowerCase() === oldChannel.toLowerCase()) return;
        const channels = (await this.ctx.storage.get("channels")) || [];
        if (!channels.some(value => String(value).toLowerCase() === destination.toLowerCase())) {
          this.safeSend(ws, { type: "relay_error", message: "The destination voice channel does not exist." });
          return;
        }
        targetInfo.voice = destination;
        targetSocket.serializeAttachment(targetInfo);
        this.safeSend(targetSocket, { type: "voice_moderation", action, targetId, oldChannel, destination, by: ownId });
        this.broadcast({ type: "voice_peer_left", peer: { id: targetInfo.id, name: targetInfo.name, voice: oldChannel } });
        this.broadcast({ type: "voice_peer_joined", peer: {
          id: targetInfo.id, name: targetInfo.name, voice: destination,
          badge: targetInfo.badge || "", avatar: targetInfo.avatar || "", avatarUrl: targetInfo.avatarUrl || ""
        } });
        this.broadcast({ type: "presence_update", peers: this.peerSnapshot() });
        return;
      }
      return;
    }

    if (data.type === "signal") {
      const to = this.clean(data.to, 64);
      const channel = this.clean(data.channel, 28);
      if (!to || !channel || !data.signal || typeof data.signal !== "object") {
        this.safeSend(ws, { type: "relay_error", message: "Invalid voice signal packet." });
        return;
      }
      if (attachment.voice !== channel) {
        this.safeSend(ws, { type: "relay_error", message: "You are not joined to this voice channel on the relay." });
        return;
      }
      const recipient = this.findSocket(to);
      const recipientInfo = recipient?.deserializeAttachment();
      if (!recipient || recipientInfo?.voice !== channel) {
        this.safeSend(ws, { type: "relay_error", message: "Voice peer is not connected to the same channel." });
        return;
      }
      this.safeSend(recipient, {
        type: "signal",
        from: ownId,
        to,
        channel,
        signal: data.signal
      });
    }
  }

  async webSocketClose(ws) {
    this.onSocketGone(ws);
  }

  async webSocketError(ws) {
    this.onSocketGone(ws);
  }

  onSocketGone(ws) {
    const attachment = ws.deserializeAttachment() || {};
    if (attachment.voice) {
      this.broadcast({ type: "voice_peer_left", peer: { id: attachment.id, name: attachment.name, voice: attachment.voice } }, ws);
    }
    this.broadcast({ type: "presence_update", peers: this.peerSnapshot(ws) }, ws);
  }

  peerSnapshot(excludeSocket = null) {
    return this.ctx.getWebSockets()
      .filter(socket => socket !== excludeSocket)
      .map(socket => {
        const info = socket.deserializeAttachment() || {};
        return {
          id: info.id || "",
          name: info.name || "",
          badge: info.badge || "",
          avatar: info.avatar || "",
          avatarUrl: info.avatarUrl || "",
          status: info.status || "",
          presence: info.presence || "online",
          voice: info.voice || "",
          voiceMuted: Boolean(info.voiceMuted),
          serverMuted: Boolean(info.serverMuted),
          deafened: Boolean(info.deafened)
        };
      })
      .filter(peer => peer.id);
  }

  findSocket(id) {
    return this.ctx.getWebSockets().find(socket => (socket.deserializeAttachment() || {}).id === id);
  }

  sendToId(id, payload) {
    const target = this.findSocket(id);
    if (target) this.safeSend(target, payload);
  }

  broadcast(payload, excludeSocket = null) {
    const serialized = JSON.stringify(payload);
    for (const socket of this.ctx.getWebSockets()) {
      if (socket === excludeSocket) continue;
      try { socket.send(serialized); } catch { /* closed sockets are cleaned by the runtime */ }
    }
  }

  safeSend(socket, payload) {
    try { socket.send(JSON.stringify(payload)); return true; }
    catch { return false; /* closed socket */ }
  }

  clean(value, maxLength) {
    return typeof value === "string" ? value.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "").trim().slice(0, maxLength) : "";
  }

  cleanPresence(value) {
    const status = this.clean(value, 12).toLowerCase();
    return ["online", "idle", "dnd"].includes(status) ? status : "online";
  }

  cleanMedia(item) {
    const url = this.clean(item?.mediaUrl, 500);
    const type = this.clean(item?.mediaType, 40).toLowerCase();
    const name = this.clean(item?.mediaName, 100);
    if (!url) return { url: "", type: "", name: "" };
    try {
      const parsed = new URL(url);
      if (!/^https?:$/.test(parsed.protocol) || !parsed.pathname.startsWith("/media/uploads/")) return { url: "", type: "", name: "" };
    } catch { return { url: "", type: "", name: "" }; }
    if (!(type.startsWith("image/") || type.startsWith("video/"))) return { url: "", type: "", name: "" };
    return { url, type, name };
  }

  cleanChannelList(value) {
    if (!Array.isArray(value)) return [];
    const list = [];
    for (const candidate of value.slice(0, 30)) {
      const name = this.clean(candidate, 28).replace(/\s+/g, " ");
      if (name && !list.some(existing => existing.toLowerCase() === name.toLowerCase())) list.push(name);
    }
    return list;
  }
}

function constantTimeEquals(a, b) {
  if (typeof a !== "string" || typeof b !== "string" || !a || !b) return false;
  let mismatch = a.length ^ b.length;
  const length = Math.max(a.length, b.length);
  for (let i = 0; i < length; i++) mismatch |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  return mismatch === 0;
}
