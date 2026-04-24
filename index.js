require("dotenv").config();  // ✅ ADDED: Load environment variables

const express = require("express");
const multer = require("multer");
const { v4: uuidv4 } = require("uuid");
const path = require("path");
const rateLimit = require('express-rate-limit');

const app = express();
const PORT = process.env.PORT || 3000;

const { PubSub } = require("@google-cloud/pubsub");
const pubsub = new PubSub();
const TOPIC_NAME = process.env.TOPIC_NAME || "file-processing-topic";

// 🔥 Firestore setup
const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

// Multer setup
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, "uploads/");
  },
  filename: function (req, file, cb) {
    const uniqueName = Date.now() + "-" + file.originalname;
    cb(null, uniqueName);
  }
});

const fileFilter = (req, file, cb) => {
  const allowedTypes = [".csv", ".pdf"];
  const ext = path.extname(file.originalname).toLowerCase();

  if (allowedTypes.includes(ext)) {
    cb(null, true);
  } else {
    cb(new Error("Only CSV and PDF files are allowed"), false);
  }
};

const upload = multer({ 
  storage, 
  fileFilter,
  limits: {
    fileSize: parseInt(process.env.MAX_FILE_SIZE) || 10 * 1024 * 1024
  }
});

// ============================================
// RATE LIMITING
// ============================================

// Global rate limit for all routes
const globalLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per windowMs
  message: { 
    error: "Too many requests", 
    message: "Please try again later.",
    retryAfter: "15 minutes"
  },
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    return req.headers['x-forwarded-for'] || req.ip;
  }
});

// Stricter limit for upload endpoints
const uploadLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 20, // 20 uploads per hour per IP
  message: { 
    error: "Upload limit exceeded", 
    message: "You have reached the maximum number of uploads per hour.",
    retryAfter: "1 hour"
  },
  standardHeaders: true,
  legacyHeaders: false
});

// Stricter limit for batch uploads
const batchUploadLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 10, // 10 batch uploads per hour per IP
  message: { 
    error: "Batch upload limit exceeded", 
    message: "You have reached the maximum number of batch uploads per hour.",
    retryAfter: "1 hour"
  },
  standardHeaders: true,
  legacyHeaders: false
});

// Apply global limiter to all routes
app.use(globalLimiter);

// Root route
app.get("/", (req, res) => {
  res.send("🚀 Data Processing API is running");
});

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({
    status: "healthy",
    timestamp: new Date(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || "development"
  });
});

// 🔥 UPLOAD API (Single file) with Rate Limiting
app.post("/upload", uploadLimiter, upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded" });
    }

    const jobId = uuidv4();
    const { webhookUrl } = req.body; // Get webhook URL from request

    await db.collection("jobs").doc(jobId).set({
      id: jobId,
      status: "pending",
      fileName: req.file.filename,
      originalName: req.file.originalname,
      fileSize: req.file.size,
      filePath: req.file.path,
      uploadedAt: new Date(),
      result: null,
      webhookUrl: webhookUrl || null,  // Store webhook URL
      progress: 0,
      statusMessage: "Job created, waiting to process..."
    });

    console.log(`✅ Job Created: ${jobId} | File: ${req.file.originalname} | Size: ${req.file.size} bytes`);

    const topic = pubsub.topic(TOPIC_NAME);
    const [exists] = await topic.exists();
    
    if (!exists) {
      console.error(`❌ Topic "${TOPIC_NAME}" does not exist!`);
      await db.collection("jobs").doc(jobId).update({
        status: "failed",
        progress: 0,
        statusMessage: "Pub/Sub topic not configured",
        result: { error: `Topic "${TOPIC_NAME}" does not exist. Create it in Google Cloud Console.` }
      });
      return res.status(500).json({ error: "Pub/Sub topic not configured" });
    }
    
    await topic.publishMessage({
      json: {
        jobId,
        fileName: req.file.filename,
        originalName: req.file.originalname,
        filePath: req.file.path,
        fileSize: req.file.size,
        uploadedAt: new Date().toISOString(),
        webhookUrl: webhookUrl || null
      }
    });
    console.log(`📤 Job sent to queue: ${jobId}`);
    
    res.status(200).json({
      success: true,
      message: "File uploaded successfully",
      jobId: jobId,
      fileName: req.file.originalname,
      fileSize: req.file.size,
      webhookUrl: webhookUrl || null
    });

  } catch (error) {
    console.error("❌ Upload Error:", error.message);
    res.status(500).json({ 
      success: false,
      error: error.message 
    });
  }
});

// 🔥 BATCH UPLOAD API (Multiple files) with Rate Limiting
app.post("/upload/batch", batchUploadLimiter, upload.array("files", 10), async (req, res) => {
  try {
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({ error: "No files uploaded" });
    }

    const jobIds = [];
    const topic = pubsub.topic(TOPIC_NAME);
    const [exists] = await topic.exists();

    if (!exists) {
      return res.status(500).json({ error: "Pub/Sub topic not configured" });
    }

    const { webhookUrl, batchId: providedBatchId } = req.body;
    const batchId = providedBatchId || uuidv4();

    for (const file of req.files) {
      const jobId = uuidv4();

      await db.collection("jobs").doc(jobId).set({
        id: jobId,
        status: "pending",
        fileName: file.filename,
        originalName: file.originalname,
        fileSize: file.size,
        filePath: file.path,
        uploadedAt: new Date(),
        result: null,
        batchId: batchId,
        webhookUrl: webhookUrl || null,
        progress: 0,
        statusMessage: "Job created, waiting to process..."
      });

      await topic.publishMessage({
        json: {
          jobId,
          fileName: file.filename,
          originalName: file.originalname,
          filePath: file.path,
          fileSize: file.size,
          batchId: batchId,
          webhookUrl: webhookUrl || null
        }
      });

      jobIds.push(jobId);
      console.log(`✅ Batch Job Created: ${jobId} | File: ${file.originalname}`);
    }

    res.status(200).json({
      success: true,
      message: `${req.files.length} files uploaded successfully`,
      batchId: batchId,
      jobIds: jobIds,
      webhookUrl: webhookUrl || null
    });

  } catch (error) {
    console.error("❌ Batch upload error:", error.message);
    res.status(500).json({ error: error.message });
  }
});

// 🔥 BATCH STATUS API (with progress)
app.get("/batch/status/:batchId", async (req, res) => {
  const { batchId } = req.params;

  try {
    const snapshot = await db.collection("jobs")
      .where("batchId", "==", batchId)
      .get();

    const jobs = [];
    snapshot.forEach(doc => {
      const job = doc.data();
      jobs.push({
        jobId: job.id,
        status: job.status,
        fileName: job.originalName,
        progress: job.progress || 0,
        statusMessage: job.statusMessage || "",
        uploadedAt: job.uploadedAt
      });
    });

    if (jobs.length === 0) {
      return res.status(404).json({ error: "Batch not found" });
    }

    const completed = jobs.filter(j => j.status === "completed").length;
    const failed = jobs.filter(j => j.status === "failed").length;
    const pending = jobs.filter(j => j.status === "pending" || j.status === "processing").length;
    
    const avgProgress = jobs.reduce((sum, j) => sum + (j.progress || 0), 0) / jobs.length;

    res.json({
      batchId: batchId,
      total: jobs.length,
      completed: completed,
      failed: failed,
      pending: pending,
      progress: Math.round((completed / jobs.length) * 100),
      avgProgress: Math.round(avgProgress),
      jobs: jobs
    });
  } catch (error) {
    console.error("❌ Batch status error:", error.message);
    res.status(500).json({ error: error.message });
  }
});

// 🔥 STATUS API (with progress tracking)
app.get("/status/:jobId", async (req, res) => {
  const { jobId } = req.params;

  try {
    const doc = await db.collection("jobs").doc(jobId).get();

    if (!doc.exists) {
      return res.status(404).json({ error: "Job not found" });
    }

    const job = doc.data();

    res.json({
      jobId: job.id,
      status: job.status,
      progress: job.progress || 0,
      statusMessage: job.statusMessage || "Starting...",
      fileName: job.originalName || job.fileName,
      uploadedAt: job.uploadedAt,
      ...(job.status === "completed" && { completedAt: job.completedAt }),
      ...(job.status === "failed" && { error: job.result?.error })
    });
  } catch (error) {
    console.error("❌ Status Error:", error.message);
    res.status(500).json({ error: "Failed to fetch status" });
  }
});

// 🔥 RESULT API
app.get("/result/:jobId", async (req, res) => {
  const { jobId } = req.params;

  try {
    const doc = await db.collection("jobs").doc(jobId).get();

    if (!doc.exists) {
      return res.status(404).json({ error: "Job not found" });
    }

    const job = doc.data();

    if (job.status !== "completed") {
      return res.status(400).json({
        message: "Job not completed yet",
        status: job.status,
        progress: job.progress || 0,
        estimatedTime: "Please wait 5-10 seconds"
      });
    }

    res.json({
      jobId: job.id,
      fileName: job.originalName || job.fileName,
      status: job.status,
      result: job.result,
      processedAt: job.completedAt || job.uploadedAt
    });
  } catch (error) {
    console.error("❌ Result Error:", error.message);
    res.status(500).json({ error: "Failed to fetch result" });
  }
});

// 🔥 LIST JOBS API
app.get("/jobs", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 10;
    const snapshot = await db.collection("jobs")
      .orderBy("uploadedAt", "desc")
      .limit(limit)
      .get();
    
    const jobs = [];
    snapshot.forEach(doc => {
      const data = doc.data();
      jobs.push({
        jobId: data.id,
        status: data.status,
        progress: data.progress || 0,
        fileName: data.originalName || data.fileName,
        uploadedAt: data.uploadedAt
      });
    });
    
    res.json({ jobs, total: jobs.length });
  } catch (error) {
    console.error("❌ List Jobs Error:", error.message);
    res.status(500).json({ error: "Failed to list jobs" });
  }
});

// Global error handler
app.use((err, req, res, next) => {
  console.error("🔥 Global Error:", err.message);
  res.status(500).json({ 
    error: err.message,
    timestamp: new Date().toISOString()
  });
});

// 404 handler for undefined routes
app.use((req, res) => {
  res.status(404).json({ error: `Route ${req.method} ${req.url} not found` });
});

// ✅ GRACEFUL SHUTDOWN for INDEX.JS
let server;

async function gracefulShutdown() {
  console.log("\n🛑 Shutting down server...");
  
  if (server) {
    server.close(async () => {
      console.log("✅ HTTP server closed");
      
      try {
        await db.terminate();
        console.log("💾 Firestore connection closed");
      } catch (err) {
        console.error("Error closing Firestore:", err.message);
      }
      
      console.log("✅ Graceful shutdown complete");
      process.exit(0);
    });
  } else {
    process.exit(0);
  }
}

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

// Start server
server = app.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════════════════════════════════╗
║     🚀 DATA PROCESSING API IS RUNNING (FULLY UPGRADED)                   ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Port:          http://localhost:${PORT}                                       ║
║  Health:        http://localhost:${PORT}/health                                 ║
║  Upload:        POST http://localhost:${PORT}/upload                            ║
║  Batch:         POST http://localhost:${PORT}/upload/batch                      ║
║  Status:        GET http://localhost:${PORT}/status/:id                         ║
║  Result:        GET http://localhost:${PORT}/result/:id                         ║
║  BatchStats:    GET http://localhost:${PORT}/batch/status/:batchId              ║
║  Jobs:          GET http://localhost:${PORT}/jobs                               ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Rate Limiting:                                                          ║
║    Global:      100 requests per 15 minutes                              ║
║    Upload:      20 uploads per hour                                      ║
║    Batch:       10 batch uploads per hour                                ║
║  Webhooks:      Supported (send webhookUrl in request body)             ║
║  Progress:      Real-time progress tracking (0-100%)                    ║
╚══════════════════════════════════════════════════════════════════════════╝
  `);
});