const cron = require('node-cron');
const { backupToTelegram, getBackupInterval } = require('./telegramBackup');

let currentJob = null;

const getCronExpression = (minutes) => {
    if (minutes <= 0) return null;
    if (minutes < 60) return `*/${minutes} * * * *`;
    const hours = Math.floor(minutes / 60);
    return `0 */${hours} * * *`;
};

const initScheduler = async () => {
    const minutes = await getBackupInterval();
    startJob(minutes);

    // Also run one backup on startup
    console.log('[Backup] Running initial backup...');
    backupToTelegram().catch(err => console.error('[Backup] Initial backup failed:', err.message));
};

const startJob = (minutes) => {
    if (currentJob) {
        currentJob.stop();
        currentJob = null;
    }

    const expr = getCronExpression(minutes);
    if (!expr) {
        console.log('[Backup] Scheduled backups disabled');
        return;
    }

    console.log(`[Backup] Scheduling every ${minutes} minutes (cron: ${expr})`);
    currentJob = cron.schedule(expr, () => {
        console.log('[Backup] Running scheduled Telegram backup...');
        backupToTelegram().catch(err => console.error('[Backup] Scheduled backup failed:', err.message));
    });
};

const reschedule = (minutes) => {
    startJob(minutes);
};

module.exports = { initScheduler, reschedule };
