const { getDatabase } = require('./dbConnector');

// Block dangerous SQL
const isDangerous = (sql) => {
    const dangerousPatterns = [
        /DROP\s+DATABASE/i,
        /ATTACH/i,
        /PRAGMA\s+writable_schema/i,
        /\.shell/i,
    ];
    return dangerousPatterns.some(pattern => pattern.test(sql));
};

const executeQuery = (dbName, sql, params = []) => {
    if (isDangerous(sql)) {
        throw new Error('Unauthorized SQL statement detected');
    }

    const db = getDatabase(dbName);
    const stmt = db.prepare(sql);
    const trimmed = sql.trim().toLowerCase();

    if (trimmed.startsWith('select') || trimmed.startsWith('pragma')) {
        return stmt.all(params);
    } else {
        const info = stmt.run(params);
        return info;
    }
};

module.exports = {
    executeQuery
};
