// Backend for the portfolio avatar: streams answers from an LLM using ONLY the verified profile. Node 18+, no dependencies.
// Environment: ANTHROPIC_API_KEY (required) · ANTHROPIC_MODEL (default claude-sonnet-5-5) · ALLOWED_ORIGIN (your site; default * for local tests only)
//              PORT (8787) · ANTHROPIC_BASE_URL (default https://api.anthropic.com; used for tests) · UPSTREAM_TIMEOUT_MS (25000)
// Run: ANTHROPIC_API_KEY=sk-... node server.js   then set AVATAR_API in the HTML to https://<host>/api/chat
const http = require('http'), fs = require('fs'), path = require('path');
const KB = JSON.parse(fs.readFileSync(path.join(__dirname, 'kb.json'), 'utf8'));
const KEY = process.env.ANTHROPIC_API_KEY, MODEL = process.env.ANTHROPIC_MODEL || 'claude-sonnet-5-5';
const BASE = (process.env.ANTHROPIC_BASE_URL || 'https://api.anthropic.com').replace(/\/$/, '');
const ORIGIN = process.env.ALLOWED_ORIGIN || '*', PORT = +process.env.PORT || 8787, TIMEOUT = +process.env.UPSTREAM_TIMEOUT_MS || 25000;
const LIMIT = 20, WINDOW = 60000, MAXHIST = 8;
const SYSTEM = [
  'You are the talking digital avatar of Shivani Poojary, an MBA graduate in Finance and Analytics. Speak in the first person, warmly and professionally, in plain spoken English suitable for text-to-speech (no markdown, no lists, no emojis).',
  'Answer open-ended questions (for example what makes her special, why hire her, strongest skills, unique aspects) by reasoning over the VERIFIED PROFILE below: select the most relevant evidence and explain it naturally.',
  'Hard rules: use only facts in the profile. Never invent qualifications, employers, dates, results, awards, skills, hobbies or personal details. Do not claim superlatives such as best or top unless the profile supports them. If the profile does not contain the answer, say so plainly and suggest emailing shivani12pvt@gmail.com. Never give a phone number.',
  'Keep answers to about 40 to 90 words unless asked for more. Use the conversation so far to resolve follow-up questions such as "tell me more" or "what about the second one".',
  '', 'VERIFIED PROFILE:', ...KB.map(e => '- ' + e.a)].join('\n');
const hits = new Map();
const limited = ip => { const n = Date.now(), a = (hits.get(ip) || []).filter(t => n - t < WINDOW); a.push(n); hits.set(ip, a); return a.length > LIMIT; };
const cors = { 'Access-Control-Allow-Origin': ORIGIN, 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Vary': 'Origin' };
const json = (res, code, obj) => { res.writeHead(code, { ...cors, 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
function cleanHistory(h) {
  if (!Array.isArray(h)) return [];
  const out = [];
  for (const m of h.slice(-MAXHIST)) {
    if (!m || (m.role !== 'user' && m.role !== 'assistant') || typeof m.content !== 'string' || !m.content.trim()) continue;
    const c = m.content.slice(0, 1200);
    if (out.length && out[out.length - 1].role === m.role) out[out.length - 1].content += ' ' + c; else out.push({ role: m.role, content: c });
  }
  while (out.length && out[0].role !== 'user') out.shift();
  if (out.length && out[out.length - 1].role === 'user') out.pop(); // the new message is appended separately
  return out;
}
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function openUpstream(messages, signal) {
  let last;
  for (let i = 0; i < 3; i++) { // retry on network errors, 429 and 5xx, only before any bytes are streamed
    try {
      const r = await fetch(BASE + '/v1/messages', { method: 'POST', signal, headers: { 'x-api-key': KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
        body: JSON.stringify({ model: MODEL, max_tokens: 400, stream: true, system: SYSTEM, messages }) });
      if (r.ok && r.body) return r;
      last = new Error('upstream ' + r.status);
      if (r.status !== 429 && r.status < 500) { last.fatal = true; throw last; }
    } catch (e) { last = e; if (e.fatal || signal.aborted) throw e; }
    await sleep(400 * (i + 1) ** 2);
  }
  throw last;
}
async function chat(req, res, body) {
  let d; try { d = JSON.parse(body); } catch { return json(res, 400, { error: 'invalid json' }); }
  const msg = d && d.message;
  if (typeof msg !== 'string' || !msg.trim() || msg.length > 300) return json(res, 400, { error: 'message must be 1-300 characters' });
  const messages = [...cleanHistory(d.history), { role: 'user', content: msg.trim() }];
  const ctl = new AbortController(), timer = setTimeout(() => ctl.abort(), TIMEOUT);
  res.on('close', () => { clearTimeout(timer); ctl.abort(); });
  let up;
  try { up = await openUpstream(messages, ctl.signal); } catch (e) { clearTimeout(timer); return json(res, ctl.signal.aborted ? 504 : 502, { error: ctl.signal.aborted ? 'upstream timeout' : 'upstream error' }); }
  res.writeHead(200, { ...cors, 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no' });
  const send = o => res.write('data: ' + JSON.stringify(o) + '\n\n');
  try {
    const dec = new TextDecoder(); let buf = '';
    for await (const chunk of up.body) {
      buf += dec.decode(chunk, { stream: true });
      let i; while ((i = buf.indexOf('\n\n')) >= 0) {
        const ev = buf.slice(0, i); buf = buf.slice(i + 2);
        const line = ev.split('\n').find(l => l.startsWith('data:')); if (!line) continue;
        let j; try { j = JSON.parse(line.slice(5)); } catch { continue; }
        if (j.type === 'content_block_delta' && j.delta && j.delta.type === 'text_delta') send({ t: j.delta.text });
        else if (j.type === 'error') throw new Error('upstream stream error');
      }
    }
    send({ done: true });
  } catch (e) { send({ error: ctl.signal.aborted ? 'timeout' : 'stream interrupted' }); }
  clearTimeout(timer); res.end();
}
http.createServer((req, res) => {
  if (req.method === 'OPTIONS') { res.writeHead(204, cors); return res.end(); }
  if (req.method === 'GET' && req.url === '/api/health') return json(res, 200, { ok: true, configured: !!KEY, model: MODEL });
  if (req.method !== 'POST' || req.url !== '/api/chat') return json(res, 404, { error: 'not found' });
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'x').toString().split(',')[0].trim();
  if (limited(ip)) return json(res, 429, { error: 'too many requests' });
  if (!KEY) return json(res, 503, { error: 'server not configured' });
  let body = ''; req.on('data', c => { body += c; if (body.length > 8192) { req.destroy(); } });
  req.on('end', () => chat(req, res, body).catch(() => { try { json(res, 500, { error: 'internal error' }); } catch {} }));
}).listen(PORT, () => console.log('avatar API on :' + PORT));
