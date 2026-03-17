const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const connections = new Map();
const DB_DIR = path.join(__dirname, '../databases');

if (!fs.existsSync(DB_DIR)) {
    fs.mkdirSync(DB_DIR, { recursive: true });
}

const getDatabase = (dbName, options = { create: false }) => {
    if (connections.has(dbName)) {
        return connections.get(dbName);
    }

    const dbPath = path.join(DB_DIR, `${dbName}.db`);
    
    if (!options.create && !fs.existsSync(dbPath)) {
        throw new Error(`Database '${dbName}' does not exist.`);
    }

    // Using better-sqlite3 with fileMustExist based on create flag
    const db = new Database(dbPath, { fileMustExist: !options.create });

    // Performance optimizations as requested in plan
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('cache_size = 100000');

    connections.set(dbName, db);
    return db;
};

const closeDatabase = (dbName) => {
    if (connections.has(dbName)) {
        const db = connections.get(dbName);
        db.close();
        connections.delete(dbName);
    }
};

const listDatabases = () => {
    return fs.readdirSync(DB_DIR)
        .filter(file => file.endsWith('.db'))
        .map(file => file.replace('.db', ''));
};

const deleteDatabase = (dbName) => {
    closeDatabase(dbName);
    const dbPath = path.join(DB_DIR, `${dbName}.db`);
    if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
        return true;
    }
    return false;
};

module.exports = {
    getDatabase,
    closeDatabase,
    listDatabases,
    deleteDatabase,
    DB_DIR
};
