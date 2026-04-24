const winston = require("winston");
const path = require("path");
const fs = require("fs");

// Ensure logs directory exists
const logsDir = path.join(__dirname, "logs");
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

// Custom format for request logging
const requestFormat = winston.format.printf(({ timestamp, level, message, requestId, method, url, status, duration, ...meta }) => {
  if (requestId) {
    return `${timestamp} ${level} [REQ:${requestId}] ${method} ${url} ${status || ""} ${duration ? `${duration}ms` : ""} - ${message}`;
  }
  return `${timestamp} ${level} ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ""}`;
});

// Define log format (JSON for files)
const jsonFormat = winston.format.combine(
  winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
  winston.format.errors({ stack: true }),
  winston.format.splat(),
  winston.format.json()
);

// Console format (human-readable with colors)
const consoleFormat = winston.format.combine(
  winston.format.colorize(),
  winston.format.timestamp({ format: "HH:mm:ss" }),
  winston.format.printf(({ timestamp, level, message, jobId, requestId, method, url, status, duration, ...meta }) => {
    let prefix = "";
    if (requestId) prefix += `[REQ:${requestId.substring(0, 8)}]`;
    if (jobId) prefix += `[JOB:${jobId.substring(0, 8)}]`;
    prefix = prefix ? `${prefix} ` : "";
    
    let metaStr = "";
    if (Object.keys(meta).length) {
      // Don't show stack traces in console
      const { stack, ...cleanMeta } = meta;
      metaStr = Object.keys(cleanMeta).length ? ` ${JSON.stringify(cleanMeta)}` : "";
    }
    
    return `${timestamp} ${level} ${prefix}${message}${metaStr}`;
  })
);

// Create logger
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || "info",
  format: jsonFormat,
  transports: [
    // Error logs to file (rotated)
    new winston.transports.File({
      filename: path.join(logsDir, "error.log"),
      level: "error",
      maxsize: 5242880, // 5MB
      maxFiles: 5,
      tailable: true,
    }),
    // All logs to file (rotated)
    new winston.transports.File({
      filename: path.join(logsDir, "combined.log"),
      maxsize: 10485760, // 10MB
      maxFiles: 5,
      tailable: true,
    }),
    // Separate request logs file
    new winston.transports.File({
      filename: path.join(logsDir, "requests.log"),
      level: "http",
      maxsize: 10485760,
      maxFiles: 3,
      format: winston.format.combine(
        winston.format.timestamp(),
        requestFormat
      )
    }),
    // Console output
    new winston.transports.Console({
      format: consoleFormat,
      level: process.env.NODE_ENV === "production" ? "info" : "debug",
    }),
  ],
});

// Create request ID middleware with performance tracking
const requestIdMiddleware = (req, res, next) => {
  const requestId = require("crypto").randomUUID();
  req.requestId = requestId;
  res.setHeader("X-Request-Id", requestId);
  
  // Track request start time
  const startTime = Date.now();
  
  // Log request on completion
  res.on("finish", () => {
    const duration = Date.now() - startTime;
    const level = res.statusCode >= 400 ? "warn" : "http";
    
    logger.log(level, `${req.method} ${req.url}`, {
      requestId,
      method: req.method,
      url: req.url,
      status: res.statusCode,
      duration,
      ip: req.ip || req.socket.remoteAddress,
      userAgent: req.get("user-agent"),
    });
  });
  
  next();
};

// Helper function to create child logger for specific job
const createJobLogger = (jobId) => {
  return {
    info: (message, meta = {}) => logger.info(message, { ...meta, jobId }),
    error: (message, meta = {}) => logger.error(message, { ...meta, jobId }),
    warn: (message, meta = {}) => logger.warn(message, { ...meta, jobId }),
    debug: (message, meta = {}) => logger.debug(message, { ...meta, jobId }),
  };
};

// Performance monitoring helper
const measureAsync = async (name, fn, context = {}) => {
  const start = Date.now();
  try {
    const result = await fn();
    const duration = Date.now() - start;
    logger.info(`Performance: ${name} completed`, { ...context, duration, status: "success" });
    return result;
  } catch (error) {
    const duration = Date.now() - start;
    logger.error(`Performance: ${name} failed`, { ...context, duration, error: error.message, status: "failed" });
    throw error;
  }
};

// Log rotation cleanup (keep last 7 days of logs)
const cleanupOldLogs = () => {
  const maxAge = 7 * 24 * 60 * 60 * 1000; // 7 days
  const now = Date.now();
  
  const logFiles = fs.readdirSync(logsDir);
  for (const file of logFiles) {
    if (file.endsWith(".log")) {
      const filePath = path.join(logsDir, file);
      const stats = fs.statSync(filePath);
      if (now - stats.mtimeMs > maxAge) {
        fs.unlinkSync(filePath);
        logger.info("Cleaned up old log file", { file, age: Math.floor((now - stats.mtimeMs) / (24 * 60 * 60 * 1000)), days: "old" });
      }
    }
  }
};

// Run cleanup once a day
setInterval(cleanupOldLogs, 24 * 60 * 60 * 1000);
cleanupOldLogs(); // Run immediately

module.exports = { 
  logger, 
  requestIdMiddleware, 
  createJobLogger,
  measureAsync 
};