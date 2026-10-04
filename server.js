// server.js
// ============ CLOUD RELAY SERVER v2 ============
// Command ID matching + result queue + device registry

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || 'Gokul_Secret_Agent_99';

// Middleware
app.use(cors());
app.use(express.json({ limit: '5mb' }));

// Storage
const commands = new Map();   // id -> {command, device, timestamp}
const results = new Map();    // id -> {result, timestamp}
const devices = new Map();    // name -> lastSeen timestamp
const MAX_AGE = 10 * 60 * 1000; // 10 min cleanup

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
}, 120000);

// API Key Authentication
function authMiddleware(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey || apiKey !== API_KEY) {
        return res.status(401).json({ error: 'Unauthorized - Wrong API Key' });
    }
    next();
}

// Health Check
app.get('/health', (req, res) => {
    const mobileSeen = devices.get('mobile');
    res.json({
        status: 'online',
        timestamp: new Date().toISOString(),
        mobile_last_seen: mobileSeen ? new Date(mobileSeen).toISOString() : null,
        pending_commands: commands.size,
        pending_results: results.size,
        devices: [...devices.keys()],
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
app.post('/send_command', authMiddleware, (req, res) => {
    const { command, device, timeout_ms } = req.body;

    if (!command) {
        return res.status(400).json({ error: 'Command is required' });
    }

    const id = crypto.randomUUID();
    commands.set(id, {
        command,
        device: device || 'mobile',
        timestamp: new Date().toISOString(),
    });

    // dashboard stats
    totalCommands += 1;
    commandHistory.push({ ts: Date.now(), command: String(command).slice(0, 60) });
    if (commandHistory.length > 200) commandHistory.shift();

    console.log(`[${new Date().toISOString()}] CMD ${id.slice(0, 8)}: ${command.slice(0, 80)}`);
    res.json({ status: 'command_sent', id });
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
app.post('/mobile_poll', authMiddleware, (req, res) => {
    devices.set('mobile', Date.now());

    // Oldest pending command eduthu delete pannu
    const oldest = [...commands.entries()]
        .filter(([, c]) => c.device === 'mobile')
        .sort((a, b) => new Date(a[1].timestamp) - new Date(b[1].timestamp))[0];

    if (oldest) {
        const [id, cmd] = oldest;
        commands.delete(id);
        return res.json({ id, command: cmd.command, timestamp: cmd.timestamp });
    }
    res.json({ command: null });
});

// Mobile → Server: Result anupum (with command ID)
app.post('/mobile_result', authMiddleware, (req, res) => {
    const { id, result } = req.body;

    if (result === undefined || result === null) {
        return res.status(400).json({ error: 'Result is required' });
    }

    results.set(id || 'legacy', {
        result: String(result).slice(0, 2000000), // 2MB cap
        timestamp: new Date().toISOString(),
    });

    totalResults += 1;

    console.log(`[${new Date().toISOString()}] RESULT ${(id || 'legacy').slice(0, 8)}: ${String(result).slice(0, 60)}`);
    res.json({ status: 'result_received' });
});

// Start
app.listen(PORT, () => {
    console.log('='.repeat(50));
    console.log('🤖 Remote AI Agent - Cloud Server v2');
    console.log('='.repeat(50));
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`✅ API Key: ${API_KEY.substring(0, 5)}...`);
    console.log(`✅ Health: http://localhost:${PORT}/health`);
    console.log('='.repeat(50));
});
