const fs = require('fs');
const path = require('path');
const { DB_DIR, closeDatabase } = require('../gateway/dbConnector');

const exportDatabase = (dbName) => {
    const filePath = path.join(DB_DIR, `${dbName}.db`);
    if (!fs.existsSync(filePath)) {
        throw new Error('Database file not found');
    }
    return filePath;
};

const importDatabase = (dbName, tempPath) => {
    const targetPath = path.join(DB_DIR, `${dbName}.db`);

    // Close existing connection if any
    closeDatabase(dbName);

    fs.copyFileSync(tempPath, targetPath);
    fs.unlinkSync(tempPath);

    return true;
};

module.exports = {
    exportDatabase,
    importDatabase
};
