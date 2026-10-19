import { DurableObject } from 'cloudflare:workers';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/') return new Response('Serveur de liaison actif — v1');
    if (url.pathname !== '/connect') return new Response('Introuvable', {status:404});
    if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') return new Response('WebSocket requis', {status:426});
    const app = url.searchParams.get('app') || '';
    const room = url.searchParams.get('room') || '';
    if (!/^[a-zA-Z0-9_-]{1,40}$/.test(app) || !/^[a-zA-Z0-9_-]{16,100}$/.test(room)) return new Response('Application ou salon invalide', {status:400});
    const id = env.ROOMS.idFromName(JSON.stringify([app,room]));
    return env.ROOMS.get(id).fetch(request);
  }
};

export class Room extends DurableObject {
  async fetch() {
    if (this.ctx.getWebSockets().length >= 8) return new Response('Salon complet', {status:429});
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    const id = crypto.randomUUID();
    server.serializeAttachment({id, start:Date.now(), count:0});
    server.send(JSON.stringify({type:'welcome', id}));
    this.presence();
    return new Response(null, {status:101, webSocket:client});
  }
  send(ws, data) { try { ws.send(JSON.stringify(data)); } catch {} }
  presence() {
    const sockets = this.ctx.getWebSockets();
    const peers = sockets.map(ws => ws.deserializeAttachment().id);
    for (const ws of sockets) this.send(ws, {type:'peers', peers});
  }
  webSocketMessage(ws, raw) {
    if (typeof raw !== 'string' || raw.length > 32768) { ws.close(1009,'Message trop grand'); return; }
    const a = ws.deserializeAttachment();
    if (Date.now()-a.start > 1000) { a.start=Date.now(); a.count=0; }
    a.count++; ws.serializeAttachment(a);
    if (a.count > 30) { ws.close(1008,'Trop de messages'); return; }
    let m; try { m=JSON.parse(raw); } catch { return; }
    if (!m || !['data','signal'].includes(m.type)) return;
    const packet = {type:m.type, from:a.id, payload:m.payload};
    for (const peer of this.ctx.getWebSockets()) {
      if (peer !== ws && (!m.to || peer.deserializeAttachment().id === m.to)) this.send(peer,packet);
    }
  }
  webSocketClose(ws, code) { try { ws.close(code); } catch {} this.presence(); }
  webSocketError(ws) { try { ws.close(1011,'Erreur'); } catch {} this.presence(); }
}
