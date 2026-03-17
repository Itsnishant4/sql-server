const fs = require('fs');
const path = require('path');
const https = require('https');
const mongoose = require('mongoose');
const { listDatabases, DB_DIR } = require('../gateway/dbConnector');

// ─── MongoDB Schemas ───
const backupSchema = new mongoose.Schema({
    db_name: { type: String, required: true },
    file_id: { type: String }, // Single part (legacy)
    parts: [{ 
        file_id: { type: String, required: true },
        size: { type: Number, required: true }
    }], // Chunked parts
    file_size: { type: Number, default: 0 },
    timestamp: { type: String, required: true },
    status: { type: String, default: 'completed' },
}, { timestamps: true });

const settingsSchema = new mongoose.Schema({
    key: { type: String, required: true, unique: true },
    value: { type: String, required: true },
});

const getBackupModel = (dbName) => {
    const modelName = `Backup_${dbName}`;
    const collectionName = `backup_${dbName}`;
    if (mongoose.models[modelName]) {
        return mongoose.models[modelName];
    }
    return mongoose.model(modelName, backupSchema, collectionName);
};

const Settings = mongoose.model('Settings', settingsSchema);

// ─── MongoDB Connection ───
let isConnected = false;

const connectMongo = async () => {
    if (isConnected) return;
    const uri = process.env.MONGODB_URI;
    if (!uri) {
        console.warn('[MongoDB] MONGODB_URI not set, backup metadata will not persist');
        return;
    }
    try {
        await mongoose.connect(uri);
        isConnected = true;
        console.log('[MongoDB] Connected for backup metadata');

        // Ensure default settings
        const existing = await Settings.findOne({ key: 'backup_interval' });
        if (!existing) {
            await Settings.create({ key: 'backup_interval', value: '30' });
        }
    } catch (err) {
        console.error('[MongoDB] Connection failed:', err.message);
    }
};

// ─── Telegram API helper ───
const telegramApi = (method, body, isFile = false) => {
    const token = process.env.TELEGRAM_BOT_TOKEN;
    if (!token) throw new Error('TELEGRAM_BOT_TOKEN not set');

    return new Promise((resolve, reject) => {
        const url = `https://api.telegram.org/bot${token}/${method}`;

        if (isFile) {
            const boundary = '----FormBoundary' + Date.now().toString(16);
            const parts = [];

            for (const [key, val] of Object.entries(body)) {
                if (val && val.stream) {
                    parts.push(
                        `--${boundary}\r\n` +
                        `Content-Disposition: form-data; name="${key}"; filename="${val.filename}"\r\n` +
                        `Content-Type: application/octet-stream\r\n\r\n`
                    );
                    parts.push(val.data);
                    parts.push('\r\n');
                } else {
                    parts.push(
                        `--${boundary}\r\n` +
                        `Content-Disposition: form-data; name="${key}"\r\n\r\n` +
                        `${val}\r\n`
                    );
                }
            }
            parts.push(`--${boundary}--\r\n`);

            let totalLength = 0;
            const buffers = parts.map(p => {
                const buf = Buffer.isBuffer(p) ? p : Buffer.from(p, 'utf8');
                totalLength += buf.length;
                return buf;
            });
            const payload = Buffer.concat(buffers, totalLength);

            const urlObj = new URL(url);
            const options = {
                hostname: urlObj.hostname,
                path: urlObj.pathname,
                method: 'POST',
                headers: {
                    'Content-Type': `multipart/form-data; boundary=${boundary}`,
                    'Content-Length': payload.length,
                },
            };

            const req = https.request(options, res => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => {
                    try { resolve(JSON.parse(data)); }
                    catch { resolve(data); }
                });
            });
            req.on('error', reject);
            req.write(payload);
            req.end();
        } else {
            const payload = JSON.stringify(body);
            const urlObj = new URL(url);
            const options = {
                hostname: urlObj.hostname,
                path: urlObj.pathname,
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(payload),
                },
            };

            const req = https.request(options, res => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => {
                    try { resolve(JSON.parse(data)); }
                    catch { resolve(data); }
                });
            });
            req.on('error', reject);
            req.write(payload);
            req.end();
        }
    });
};

// ─── Backup to Telegram ───
const backupToTelegram = async (dbToBackup = null) => {
    const chatId = process.env.TELEGRAM_CHAT_ID;
    const token = process.env.TELEGRAM_BOT_TOKEN;

    if (!chatId || !token) {
        console.warn('[Backup] Skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set');
        return { success: false, error: 'Telegram not configured' };
    }

    const CHUNK_SIZE = 19 * 1024 * 1024; // 19MB (Telegram download limit is 20MB)

    await connectMongo();
    let dbs = listDatabases().filter(n => !n.startsWith('_'));
    if (dbToBackup) {
        dbs = dbs.filter(n => n === dbToBackup);
    }
    const results = [];

    for (const dbName of dbs) {
        const filePath = path.join(DB_DIR, `${dbName}.db`);
        try {
            const fileData = fs.readFileSync(filePath);
            const fileSize = fileData.length;
            const timestamp = new Date().toISOString();
            
            const parts = [];
            const isChunked = fileSize > CHUNK_SIZE;
            
            if (!isChunked) {
                // Original single-part backup
                const caption = `📦 Backup: ${dbName}\n📅 ${timestamp}`;
                const result = await telegramApi('sendDocument', {
                    chat_id: chatId,
                    caption: caption,
                    document: {
                        stream: true,
                        filename: `${dbName}_${Date.now()}.db`,
                        data: fileData,
                    },
                }, true);

                if (result.ok) {
                    const BackupModel = getBackupModel(dbName);
                    await BackupModel.create({
                        db_name: dbName,
                        file_id: result.result.document.file_id,
                        file_size: fileSize,
                        timestamp,
                        status: 'completed',
                    });
                    console.log(`✅ Backed up ${dbName} to Telegram (Single part)`);
                    results.push({ db: dbName, status: 'ok' });
                } else {
                    throw new Error(result.description);
                }
            } else {
                // Chunked backup
                const totalParts = Math.ceil(fileSize / CHUNK_SIZE);
                console.log(`📦 Splitting ${dbName} (${(fileSize/1024/1024).toFixed(2)}MB) into ${totalParts} parts...`);

                for (let i = 0; i < totalParts; i++) {
                    const start = i * CHUNK_SIZE;
                    const end = Math.min(start + CHUNK_SIZE, fileSize);
                    const chunk = fileData.slice(start, end);
                    
                    const caption = `📦 Backup: ${dbName} (Part ${i + 1}/${totalParts})\n📅 ${timestamp}`;
                    const result = await telegramApi('sendDocument', {
                        chat_id: chatId,
                        caption: caption,
                        document: {
                            stream: true,
                            filename: `${dbName}_${Date.now()}.part${i + 1}`,
                            data: chunk,
                        },
                    }, true);

                    if (result.ok) {
                        parts.push({
                            file_id: result.result.document.file_id,
                            size: chunk.length
                        });
                    } else {
                        throw new Error(`Failed to upload part ${i+1}: ${result.description}`);
                    }
                }

                const BackupModel = getBackupModel(dbName);
                await BackupModel.create({
                    db_name: dbName,
                    parts: parts,
                    file_size: fileSize,
                    timestamp,
                    status: 'completed',
                });

                console.log(`✅ Backed up ${dbName} to Telegram (${totalParts} parts)`);
                results.push({ db: dbName, status: 'ok', parts: totalParts });
            }
        } catch (error) {
            console.error(`❌ Backup error for ${dbName}:`, error.message);
            results.push({ db: dbName, status: 'error', error: error.message });
        }
    }

    return { success: true, results };
};

// ─── List backups from MongoDB ───
const listBackups = async (dbName) => {
    await connectMongo();
    const fetchFromModel = async (Model) => {
        const raw = await Model.find().sort({ _id: -1 }).limit(100).lean();
        const grouped = [];
        const processedIds = new Set();

        for (const item of raw) {
            if (processedIds.has(item._id.toString())) continue;

            const partMatch = item.status && item.status.match(/part(\d+)\/(\d+)/);
            if (partMatch && !item.parts) {
                const [_, current, total] = partMatch;
                // It's an old-style multi-part backup. Try to find siblings.
                const tsDate = new Date(item.timestamp);
                const siblings = raw.filter(s => 
                    s.db_name === item.db_name && 
                    s.status && s.status.includes(`/${total}`) &&
                    Math.abs(new Date(s.timestamp) - tsDate) < 10 * 60 * 1000 // 10 min window
                ).sort((a, b) => {
                    const ma = a.status.match(/part(\d+)/);
                    const mb = b.status.match(/part(\d+)/);
                    return (ma ? parseInt(ma[1]) : 0) - (mb ? parseInt(mb[1]) : 0);
                });

                if (siblings.length > 0) {
                    const totalSize = siblings.reduce((sum, s) => sum + (s.file_size || 0), 0);
                    const allParts = siblings.map(s => ({
                        file_id: s.file_id,
                        size: s.file_size || 0,
                        _id: s._id
                    }));
                    
                    grouped.push({
                        ...siblings[0],
                        _id: siblings[0]._id, // Use the first part's ID as the main ID
                        file_size: totalSize,
                        parts: allParts,
                        status: `completed (${siblings.length}/${total} parts detected)`,
                        is_aggregated: true
                    });

                    siblings.forEach(s => processedIds.add(s._id.toString()));
                } else {
                    grouped.push(item);
                    processedIds.add(item._id.toString());
                }
            } else {
                grouped.push(item);
                processedIds.add(item._id.toString());
            }
        }
        return grouped;
    };

    if (dbName) {
        const BackupModel = getBackupModel(dbName);
        return await fetchFromModel(BackupModel);
    }
    
    const collections = await mongoose.connection.db.listCollections({ name: /^backup_/ }).toArray();
    let allBackups = [];
    for (const coll of collections) {
        const name = coll.name.replace('backup_', '');
        const BackupModel = getBackupModel(name);
        const backups = await fetchFromModel(BackupModel);
        allBackups = allBackups.concat(backups);
    }
    return allBackups.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
};

// ─── Restore a backup from Telegram ───
const restoreBackup = async (backupId, dbName) => {
    const token = process.env.TELEGRAM_BOT_TOKEN;
    if (!token) throw new Error('TELEGRAM_BOT_TOKEN not set');

    await connectMongo();
    if (!dbName) throw new Error('dbName is required for restore');
    
    const BackupModel = getBackupModel(dbName);
    let backup = await BackupModel.findById(backupId).lean();
    
    // If not found, it might be an aggregated old-style backup that we virtualized
    if (!backup) {
        // Find if this ID is part of a group
        const all = await BackupModel.find().lean();
        const found = all.find(item => item._id.toString() === backupId);
        if (found) {
            // We found the "anchor" document. Now let's see if we need siblings.
            const partMatch = found.status && found.status.match(/part(\d+)\/(\d+)/);
            if (partMatch && !found.parts) {
                const [_, current, total] = partMatch;
                const tsDate = new Date(found.timestamp);
                const siblings = all.filter(s => 
                    s.db_name === found.db_name && 
                    s.status && s.status.includes(`/${total}`) &&
                    Math.abs(new Date(s.timestamp) - tsDate) < 10 * 60 * 1000
                ).sort((a, b) => {
                    const ma = a.status.match(/part(\d+)/);
                    const mb = b.status.match(/part(\d+)/);
                    return (ma ? parseInt(ma[1]) : 0) - (mb ? parseInt(mb[1]) : 0);
                });
                
                backup = {
                    ...found,
                    parts: siblings.map(s => ({ file_id: s.file_id, size: s.file_size || 0 }))
                };
            } else {
                backup = found;
            }
        }
    }

    if (!backup) throw new Error('Backup not found in collection ' + dbName);

    const downloadPart = async (fileId) => {
        const fileInfo = await telegramApi('getFile', { file_id: fileId });
        if (!fileInfo.ok) throw new Error('Failed to get file from Telegram: ' + fileInfo.description);

        const downloadUrl = `https://api.telegram.org/file/bot${token}/${fileInfo.result.file_path}`;
        return new Promise((resolve, reject) => {
            https.get(downloadUrl, res => {
                const chunks = [];
                res.on('data', chunk => chunks.push(chunk));
                res.on('end', () => resolve(Buffer.concat(chunks)));
                res.on('error', reject);
            }).on('error', reject);
        });
    };

    let finalData;
    if (backup.parts && backup.parts.length > 0) {
        console.log(`📦 Reassembling ${backup.db_name} from ${backup.parts.length} parts...`);
        const partBuffers = [];
        for (const part of backup.parts) {
            partBuffers.push(await downloadPart(part.file_id));
        }
        finalData = Buffer.concat(partBuffers);
    } else {
        if (!backup.file_id) throw new Error('Backup metadata is missing file_id');
        finalData = await downloadPart(backup.file_id);
    }

    // Close existing connection and write
    const { closeDatabase } = require('../gateway/dbConnector');
    closeDatabase(backup.db_name);

    const destPath = path.join(DB_DIR, `${backup.db_name}.db`);
    fs.writeFileSync(destPath, finalData);
    console.log(`✅ Restored ${backup.db_name} from backup ${backupId}`);

    return { success: true, db_name: backup.db_name, timestamp: backup.timestamp };
};

// ─── Settings (from MongoDB) ───
const getBackupInterval = async () => {
    await connectMongo();
    const row = await Settings.findOne({ key: 'backup_interval' }).lean();
    return row ? parseInt(row.value) : 30;
};

const setBackupInterval = async (minutes) => {
    await connectMongo();
    await Settings.findOneAndUpdate(
        { key: 'backup_interval' },
        { value: String(minutes) },
        { upsert: true }
    );
    return minutes;
};

const performInitialRestore = async () => {
    await connectMongo();
    if (!isConnected) return; // No MongoDB, no backups

    const { listDatabases } = require('../gateway/dbConnector');
    
    // Check if initial restore was already processed
    const done = await Settings.findOne({ key: 'initial_restore_done' }).lean();
    if (done) return; // Already did this in the past

    // Mark as done immediately so we never do it again in future runs, even if DBs are deleted
    await Settings.findOneAndUpdate(
        { key: 'initial_restore_done' },
        { value: 'true' },
        { upsert: true }
    );

    // If local databases already exist, don't overwrite or restore anything
    const localDbs = listDatabases().filter(n => !n.startsWith('_'));
    if (localDbs.length > 0) return;

    try {
        console.log('🔄 [Initial Restore] No local databases found. Checking for remote backups...');
        
        const latestBackups = await listBackups();
        if (!latestBackups || latestBackups.length === 0) {
            console.log('ℹ️ [Initial Restore] No existing backups found.');
            return;
        }

        // We only want the latest backup per database. listBackups already sorts by timestamp desc,
        // so we can just grab the first one we see for each db_name.
        const seenDbs = new Set();
        let restoredCount = 0;

        for (const backup of latestBackups) {
            if (!seenDbs.has(backup.db_name) && backup.status.includes('completed')) {
                seenDbs.add(backup.db_name);
                console.log(`⏳ [Initial Restore] Restoring latest backup for "${backup.db_name}"...`);
                try {
                    await restoreBackup(backup._id.toString(), backup.db_name);
                    restoredCount++;
                } catch (err) {
                    console.error(`❌ [Initial Restore] Failed to restore "${backup.db_name}":`, err.message);
                }
            }
        }
        
        if (restoredCount > 0) {
            console.log(`✅ [Initial Restore] Successfully restored ${restoredCount} databases on first run.`);
        }
    } catch (err) {
        console.error('❌ [Initial Restore] Error during initial restore process:', err.message);
    }
};

module.exports = { backupToTelegram, listBackups, restoreBackup, getBackupInterval, setBackupInterval, connectMongo, performInitialRestore };
