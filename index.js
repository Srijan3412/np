const express = require("express");
const multer = require("multer");
const { v4: uuidv4 } = require("uuid");
const path = require("path");

const app = express();
const PORT = 3000;

const { PubSub } = require("@google-cloud/pubsub");
const pubsub = new PubSub();
const TOPIC_NAME = "file-processing-topic";

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

const upload = multer({ storage, fileFilter });

// Root
app.get("/", (req, res) => {
  res.send("🚀 Data Processing API is running");
});

// 🔥 UPLOAD API
app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded" });
    }

    const jobId = uuidv4();

    // ✅ Save to Firestore
    await db.collection("jobs").doc(jobId).set({
      id: jobId,
      status: "pending",
      fileName: req.file.filename,
      filePath: req.file.path,
      uploadedAt: new Date(),
      result: null
    });

    console.log("✅ Job Created:", jobId);

    // ✅ SEND TO PUB/SUB (REPLACEMENT FOR setTimeout)
 
 
    try {
    await pubsub.topic(TOPIC_NAME).publishMessage({
      json: {
        jobId,
        fileName: req.file.filename,
        filePath: req.file.path
      }
    });
    } catch (err) {
      console.error("❌ PubSub error:", err.message);
    }
    console.log("📤 Job sent to queue:", jobId);
    
    res.status(200).json({
      message: "File uploaded successfully",
      jobId
    });

  } catch (error) {
    console.error("❌ Upload Error:", error.message);
    res.status(500).json({ error: error.message });
  }
});

// 🔥 STATUS API
app.get("/status/:jobId", async (req, res) => {
  const { jobId } = req.params;

  const doc = await db.collection("jobs").doc(jobId).get();

  if (!doc.exists) {
    return res.status(404).json({ error: "Job not found" });
  }

  const job = doc.data();

  res.json({
    jobId: job.id,
    status: job.status,
    uploadedAt: job.uploadedAt
  });
});

// 🔥 RESULT API
app.get("/result/:jobId", async (req, res) => {
  const { jobId } = req.params;

  const doc = await db.collection("jobs").doc(jobId).get();

  if (!doc.exists) {
    return res.status(404).json({ error: "Job not found" });
  }

  const job = doc.data();

  if (job.status !== "completed") {
    return res.status(400).json({
      message: "Job not completed yet",
      status: job.status
    });
  }

  res.json({
    jobId: job.id,
    result: job.result
  });
});

// Global error handler
app.use((err, req, res, next) => {
  console.error("🔥 Global Error:", err.message);
  res.status(500).json({ error: err.message });
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});