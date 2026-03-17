const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const dotenv = require('dotenv');
const multer = require('multer');
const path = require('path');
const authMiddleware = require('./security/authMiddleware');
const { executeQuery } = require('./gateway/queryRouter');
const { listDatabases, deleteDatabase, getDatabase } = require('./gateway/dbConnector');
const { exportDatabase, importDatabase } = require('./importExport/dbIO');
const { initScheduler, reschedule } = require('./backup/scheduler');
const { backupToTelegram, listBackups, restoreBackup, getBackupInterval, setBackupInterval } = require('./backup/telegramBackup');
const os = require('os'); // Added for system metrics

dotenv.config();

let totalRequests = 0; // Request counter

const app = express();
const PORT = process.env.PORT || 8081;
const upload = multer({ dest: 'uploads/' });

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', req.headers.origin || '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-db-name, Accept, Origin, X-Requested-With');
    res.header('Access-Control-Allow-Credentials', 'true');
    
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});
app.use(morgan('dev'));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

app.use((req, res, next) => {
    // Increment total requests counter
    totalRequests++;

    // Log auth header presence (avoid logging full token for security)
    const authHeader = req.headers.authorization;
    if (authHeader) {
        console.log(`[Auth] ${req.method} ${req.url} - Token present: ${authHeader.slice(0, 15)}...`);
    } else {
        console.log(`[Auth] ${req.method} ${req.url} - No token found`);
    }

    if (req.url.includes('v2/pipeline')) {
        const dbName = req.params.db || req.headers['x-db-name'] || (req.path.split('/')[1] === 'v2' ? 'main' : req.path.split('/')[1]);
        console.log(`[libSQL] ${req.method} ${req.url} - DB: ${dbName}`);
    }
    next();
});

// Public health check
app.get('/health', (req, res) => res.json({ status: 'ok' }));

// System Metrics
app.get('/system/metrics', (req, res) => {
    const memUsage = process.memoryUsage();
    res.json({
        success: true,
        data: {
            uptime: process.uptime(), // Node.js process uptime in seconds
            totalRequests: totalRequests,
            memory: {
                rss: memUsage.rss,
                heapTotal: memUsage.heapTotal,
                heapUsed: memUsage.heapUsed,
                external: memUsage.external,
            },
            system: {
                loadavg: os.loadavg(), // Array containing the 1, 5, and 15 minute load averages
                freemem: os.freemem(),
                totalmem: os.totalmem(),
                cpus: os.cpus().length
            }
        }
    });
});

// Auth protected routes
app.use(authMiddleware);

// ─────────────────── libSQL Compatibility Layer ───────────────────
const executeStmt = (db, stmtObj) => {
    const { sql, args } = stmtObj;
    const params = (args || []).map(a => (typeof a === 'object' && a !== null) ? a.value : a);
    const result = executeQuery(db, sql, params);
    const isSelect = Array.isArray(result);
    const rows = isSelect ? result : [];
    const cols = rows.length > 0 ? Object.keys(rows[0]).map(name => ({ name })) : [];

    return {
        cols,
        rows: rows.map(row =>
            Object.values(row).map(v => {
                if (v === null) return { type: 'null' };
                if (typeof v === 'number') {
                    if (Number.isInteger(v)) return { type: 'integer', value: String(v) };
                    return { type: 'float', value: v };
                }
                return { type: 'text', value: String(v) };
            })
        ),
        affected_row_count: isSelect ? 0 : (result.changes || 0),
        last_insert_rowid: isSelect ? null : (result.lastInsertRowid ? String(result.lastInsertRowid) : null)
    };
};

const handlePipeline = (db, requests) => {
    const results = requests.map(req => {
        if (req.type === 'execute') {
            try {
                const result = executeStmt(db, req.stmt);
                return { type: 'ok', response: { type: 'execute', result } };
            } catch (error) {
                return { type: 'error', error: { message: error.message, code: 'SQL_ERROR' } };
            }
        } else if (req.type === 'batch') {
            try {
                const stepResults = [];
                const stepErrors = [];
                const steps = req.batch.steps || [];

                const evaluateCond = (cond) => {
                    if (!cond) return true;
                    if (cond.type === 'ok') return stepResults[cond.step] !== null && stepErrors[cond.step] === null;
                    if (cond.type === 'error') return stepErrors[cond.step] !== null;
                    if (cond.type === 'not') return !evaluateCond(cond.cond);
                    if (cond.type === 'and') return (cond.conds || []).every(c => evaluateCond(c));
                    if (cond.type === 'or') return (cond.conds || []).some(c => evaluateCond(c));
                    if (cond.type === 'is_autocommit') {
                        const dbInstance = getDatabase(db);
                        return !dbInstance.inTransaction;
                    }
                    return true;
                };

                for (let i = 0; i < steps.length; i++) {
                    const step = steps[i];
                    try {
                        if (evaluateCond(step.condition)) {
                            stepResults.push(executeStmt(db, step.stmt));
                            stepErrors.push(null);
                        } else {
                            stepResults.push(null);
                            stepErrors.push(null);
                        }
                    } catch (error) {
                        stepResults.push(null);
                        stepErrors.push({ message: error.message, code: 'SQL_ERROR' });
                    }
                }

                return { type: 'ok', response: { type: 'batch', result: { step_results: stepResults, step_errors: stepErrors } } };
            } catch (error) {
                return { type: 'error', error: { message: error.message, code: 'BATCH_ERROR' } };
            }
        } else if (req.type === 'close') {
            return { type: 'ok', response: { type: 'close' } };
        } else if (req.type === 'sequence') {
            return { type: 'ok', response: { type: 'sequence' } };
        } else if (req.type === 'describe') {
            return { type: 'ok', response: { type: 'describe', result: { params: [], cols: [], is_explain: false, is_readonly: false } } };
        } else if (req.type === 'store_sql' || req.type === 'close_sql') {
            return { type: 'ok', response: { type: req.type } };
        }
        return { type: 'ok', response: { type: req.type } };
    });
    return { results, baton: null, base_url: null };
};

app.post('/v2/pipeline', (req, res) => {
    const db = req.headers['x-db-name'] || 'main';
    res.json(handlePipeline(db, req.body.requests || []));
});
app.post('/:db/v2/pipeline', (req, res) => {
    res.json(handlePipeline(req.params.db, req.body.requests || []));
});

// Original query format
app.post('/query/:db', (req, res) => {
    const { db } = req.params;
    const { sql, params } = req.body;
    try {
        const result = executeQuery(db, sql, params || []);
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

// ─────────────────── Database Management ───────────────────
app.get('/databases', (req, res) => {
    try {
        const dbs = listDatabases().filter(n => !n.startsWith('_'));
        res.json({ success: true, data: dbs });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});
app.post('/databases/:db', (req, res) => {
    try { getDatabase(req.params.db); res.json({ success: true, message: `Database ${req.params.db} created` }); }
    catch (error) { res.status(500).json({ success: false, error: error.message }); }
});
app.delete('/databases/:db', (req, res) => {
    try {
        if (deleteDatabase(req.params.db)) res.json({ success: true });
        else res.status(404).json({ success: false, error: 'Not found' });
    } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// ─────────────────── Table Management ───────────────────
// List tables with row counts & columns
app.get('/tables/:db', (req, res) => {
    const { db } = req.params;
    try {
        const tables = executeQuery(db, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
        const result = tables.map(t => {
            const countRow = executeQuery(db, `SELECT COUNT(*) as count FROM "${t.name}"`);
            const columns = executeQuery(db, `PRAGMA table_info("${t.name}")`);
            const foreignKeys = executeQuery(db, `PRAGMA foreign_key_list("${t.name}")`);
            return {
                name: t.name,
                row_count: countRow[0]?.count || 0,
                columns: columns.map(c => ({
                    name: c.name,
                    type: c.type,
                    notnull: c.notnull === 1,
                    pk: c.pk === 1,
                    default_value: c.dflt_value,
                })),
                foreign_keys: foreignKeys.map(fk => ({
                    from: fk.from,
                    to_table: fk.table,
                    to_column: fk.to,
                    on_update: fk.on_update,
                    on_delete: fk.on_delete
                }))
            };
        });
        res.json({ success: true, data: result });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

// Create table
app.post('/tables/:db', (req, res) => {
    const { db } = req.params;
    const { name, columns } = req.body;
    // columns: [{ name, type, pk?, notnull?, default_value? }]
    if (!name || !columns || columns.length === 0) {
        return res.status(400).json({ success: false, error: 'Table name and columns required' });
    }
    try {
        const colDefs = columns.map(c => {
            let def = `"${c.name}" ${c.type}`;
            if (c.pk) def += ' PRIMARY KEY';
            if (c.notnull) def += ' NOT NULL';
            if (c.default_value !== undefined && c.default_value !== '') def += ` DEFAULT ${c.default_value}`;
            return def;
        }).join(', ');
        executeQuery(db, `CREATE TABLE "${name}" (${colDefs})`);
        res.json({ success: true, message: `Table ${name} created` });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

// Drop table
app.delete('/tables/:db/:table', (req, res) => {
    try {
        executeQuery(req.params.db, `DROP TABLE IF EXISTS "${req.params.table}"`);
        res.json({ success: true });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

// ─────────────────── Row CRUD ───────────────────
// Get data with pagination & filters
app.get('/tables/:db/:table/data', (req, res) => {
    const { db, table } = req.params;
    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 50, 500);
    const offset = (page - 1) * limit;
    const sortCol = req.query.sort || 'rowid';
    const sortDir = req.query.dir === 'desc' ? 'DESC' : 'ASC';

    // Filters: ?filter_col=name&filter_op=eq&filter_val=test (can have multiple)
    let filters = [];
    if (req.query.filters) {
        try {
            filters = JSON.parse(req.query.filters); // [{ column, op, value }]
        } catch {}
    }

    try {
        let whereClause = '';
        const params = [];
        if (filters.length > 0) {
            const conditions = filters.map(f => {
                const col = `"${f.column}"`;
                switch (f.op) {
                    case 'eq': params.push(f.value); return `${col} = ?`;
                    case 'neq': params.push(f.value); return `${col} != ?`;
                    case 'gt': params.push(f.value); return `${col} > ?`;
                    case 'gte': params.push(f.value); return `${col} >= ?`;
                    case 'lt': params.push(f.value); return `${col} < ?`;
                    case 'lte': params.push(f.value); return `${col} <= ?`;
                    case 'like': params.push(`%${f.value}%`); return `${col} LIKE ?`;
                    case 'is_null': return `${col} IS NULL`;
                    case 'is_not_null': return `${col} IS NOT NULL`;
                    default: params.push(f.value); return `${col} = ?`;
                }
            });
            whereClause = 'WHERE ' + conditions.join(' AND ');
        }

        const countResult = executeQuery(db, `SELECT COUNT(*) as total FROM "${table}" ${whereClause}`, params);
        const total = countResult[0]?.total || 0;
        const data = executeQuery(db, `SELECT rowid, * FROM "${table}" ${whereClause} ORDER BY "${sortCol}" ${sortDir} LIMIT ? OFFSET ?`, [...params, limit, offset]);
        const columns = executeQuery(db, `PRAGMA table_info("${table}")`);

        res.json({
            success: true,
            data,
            pagination: { page, limit, total, totalPages: Math.ceil(total / limit) },
            columns: columns.map(c => ({ name: c.name, type: c.type, notnull: c.notnull === 1, pk: c.pk === 1 })),
        });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

// Insert row
app.post('/tables/:db/:table/rows', (req, res) => {
    const { db, table } = req.params;
    const { row } = req.body; // { col1: val1, col2: val2, ... }
    if (!row || Object.keys(row).length === 0) {
        return res.status(400).json({ success: false, error: 'Row data required' });
    }
    try {
        const keys = Object.keys(row);
        const placeholders = keys.map(() => '?').join(', ');
        const sql = `INSERT INTO "${table}" (${keys.map(k => `"${k}"`).join(', ')}) VALUES (${placeholders})`;
        const result = executeQuery(db, sql, Object.values(row));
        res.json({ success: true, lastInsertRowid: result.lastInsertRowid });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

// Update row
app.put('/tables/:db/:table/rows', (req, res) => {
    const { db, table } = req.params;
    const { rowid, updates } = req.body; // updates: { col1: newVal, ... }
    if (!rowid || !updates) {
        return res.status(400).json({ success: false, error: 'rowid and updates required' });
    }
    try {
        const sets = Object.keys(updates).map(k => `"${k}" = ?`).join(', ');
        const sql = `UPDATE "${table}" SET ${sets} WHERE rowid = ?`;
        executeQuery(db, sql, [...Object.values(updates), rowid]);
        res.json({ success: true });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

// Delete row
app.delete('/tables/:db/:table/rows', (req, res) => {
    const { db, table } = req.params;
    const { rowid } = req.body;
    if (!rowid) {
        return res.status(400).json({ success: false, error: 'rowid is required in the request body' });
    }
    try {
        executeQuery(db, `DELETE FROM "${table}" WHERE rowid = ?`, [rowid]);
        res.json({ success: true });
    } catch (error) {
        res.status(400).json({ success: false, error: error.message });
    }
});

// ─────────────────── Backup Management ───────────────────
app.get('/backups', async (req, res) => {
    try { res.json({ success: true, data: await listBackups(req.query.db) }); }
    catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.post('/backups/now', async (req, res) => {
    try {
        const result = await backupToTelegram(req.query.db);
        res.json({ success: true, ...result });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/backups/restore/:id', async (req, res) => {
    try {
        const result = await restoreBackup(req.params.id, req.query.db);
        res.json(result);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/backups/settings', async (req, res) => {
    try {
        const interval = await getBackupInterval();
        const configured = !!(process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID);
        res.json({ success: true, interval, telegram_configured: configured });
    } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.put('/backups/settings', async (req, res) => {
    const { interval } = req.body;
    if (!interval || interval < 1) return res.status(400).json({ success: false, error: 'Invalid interval' });
    try {
        await setBackupInterval(interval);
        reschedule(interval);
        res.json({ success: true, interval });
    } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// ─────────────────── Import/Export ───────────────────
app.get('/export/:db', (req, res) => {
    try { res.download(exportDatabase(req.params.db)); }
    catch (error) { res.status(404).json({ success: false, error: error.message }); }
});

app.post('/import/:db', upload.single('database'), (req, res) => {
    if (!req.file) return res.status(400).json({ success: false, error: 'No file uploaded' });
    try {
        importDatabase(req.params.db, req.file.path);
        res.json({ success: true, message: `Database ${req.params.db} imported` });
    } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// Direct database pipeline (some clients use this directly)
app.post('/:db', (req, res) => {
    if (req.body && req.body.requests) {
        return res.json(handlePipeline(req.params.db, req.body.requests));
    }
    res.status(404).json({ success: false, error: 'Not found' });
});

// ─────────────────── Start ───────────────────
const start = async () => {
    initScheduler();
    app.listen(PORT, () => {
        console.log(`SQLite Cloud Gateway running on port ${PORT}`);
    });
};

start().catch(err => {
    console.error('Failed to start server:', err);
    process.exit(1);
});
