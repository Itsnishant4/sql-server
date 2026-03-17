const dotenv = require('dotenv');

dotenv.config();

const secret = process.env.DB_SECRET;
console.log(secret);
const authMiddleware = (req, res, next) => {
    const authHeader = req.headers.authorization;
    const token = authHeader && authHeader.split(' ')[1];

    // Allow OPTIONS requests (CORS preflight)
    if (req.method === 'OPTIONS') {
        return next();
    }

    if (!token) {
        return res.status(401).json({ success: false, error: 'Authentication token required' });
    }
 
    if (token !== secret) {
        return res.status(403).json({ success: false, error: 'Invalid authentication token' });
    }

    next();
};

module.exports = authMiddleware;
