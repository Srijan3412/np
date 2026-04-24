const { PubSub } = require("@google-cloud/pubsub");
const admin = require("firebase-admin");
const fs = require("fs");
const csv = require("csv-parser");
const pdfParse = require("pdf-parse");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const { logger, createJobLogger, measureAsync } = require("./logger");
require("dotenv").config();

// ============================================
// CONFIGURATION
// ============================================
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT) || 5;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 10;
const QUEUE_SIZE = parseInt(process.env.QUEUE_SIZE) || 50;

let activeProcesses = 0;
let queue = [];
let totalProcessed = 0;
let totalFailed = 0;
let startTime = Date.now();

// Set your project ID
process.env.GOOGLE_CLOUD_PROJECT = "smpi-3f14b";

// Initialize Gemini AI
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const aiModel = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });

const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

// Initialize PubSub
const pubsub = new PubSub({
  projectId: "smpi-3f14b",
  credentials: serviceAccount
});

const subscriptionName = "file-processing-sub";
const subscription = pubsub.subscription(subscriptionName);

// ============================================
// GEMINI AI PARSING FUNCTION
// ============================================
async function parseWithGemini(text, fileName) {
  const limitedText = text.substring(0, 15000);
  
  const prompt = `You are a document analyzer. Analyze this document and return ONLY valid JSON (no other text).

Document filename: ${fileName}
Document content: ${limitedText}

Extract and return this exact JSON structure:
{
  "documentType": "one of: Resume, Invoice, Report, Contract, Letter, Scientific Paper, Legal Document, Financial, Educational, Other",
  "confidence": "high/medium/low",
  "keyInformation": {
    "name": "person name if found",
    "email": "email if found",
    "phone": "phone number if found",
    "organization": "company/organization name if found",
    "date": "important date if found"
  },
  "summary": "one sentence summary of the document (max 150 chars)",
  "mainSections": ["section1", "section2", "section3"],
  "keyTopics": ["topic1", "topic2", "topic3", "topic4", "topic5"],
  "sentiment": "positive/negative/neutral"
}

Return ONLY the JSON. No explanations, no markdown.`;
  
  try {
    const result = await aiModel.generateContent(prompt);
    const responseText = result.response.text();
    
    let cleanJson = responseText;
    if (cleanJson.includes("```json")) {
      cleanJson = cleanJson.split("```json")[1].split("```")[0];
    } else if (cleanJson.includes("```")) {
      cleanJson = cleanJson.split("```")[1].split("```")[0];
    }
    
    return JSON.parse(cleanJson);
  } catch (err) {
    logger.error("Gemini parsing error", { error: err.message, fileName });
    return {
      documentType: "Unknown",
      confidence: "low",
      keyInformation: {},
      summary: text.substring(0, 150),
      mainSections: [],
      keyTopics: [],
      sentiment: "neutral"
    };
  }
}

// ============================================
// BASIC FALLBACK PARSING
// ============================================
function basicParse(text, fileName) {
  const cleanText = text.replace(/\s+/g, ' ').trim();
  const wordCount = cleanText.split(/\s+/).length;
  
  const emails = cleanText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g) || [];
  const urls = cleanText.match(/https?:\/\/[^\s]+/g) || [];
  const phones = cleanText.match(/[\+]?[(]?[0-9]{1,3}[)]?[-\s\.]?[(]?[0-9]{1,4}[)]?[-\s\.]?[0-9]{1,4}[-\s\.]?[0-9]{1,9}/g) || [];
  
  return {
    documentType: "Unknown",
    confidence: "low",
    keyInformation: {
      email: emails[0] || null,
      phone: phones[0] || null,
      url: urls[0] || null
    },
    summary: cleanText.substring(0, 200),
    mainSections: [],
    keyTopics: [],
    sentiment: "neutral",
    wordCount: wordCount,
    emails: emails.slice(0, 5),
    urls: urls.slice(0, 5)
  };
}

// ============================================
// SINGLE FILE PROCESSOR
// ============================================
async function processFile(jobId, filePath, fileName, message, batchId = null) {
  const jobLogger = createJobLogger(jobId);
  activeProcesses++;
  
  const startTime = Date.now();
  jobLogger.info("Processing started", { 
    fileName, 
    activeConcurrent: activeProcesses,
    queueLength: queue.length,
    batchId 
  });

  try {
    await db.collection("jobs").doc(jobId).update({
      status: "processing",
      processingStartedAt: new Date(),
      ...(batchId && { batchId })
    });

    // CSV Processing
    if (fileName.endsWith(".csv")) {
      let rowCount = 0;
      let columns = [];

      await new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
          .pipe(csv())
          .on("headers", (headers) => {
            columns = headers;
            jobLogger.debug("CSV headers detected", { columnCount: headers.length });
          })
          .on("data", () => rowCount++)
          .on("end", async () => {
            await db.collection("jobs").doc(jobId).update({
              status: "completed",
              completedAt: new Date(),
              result: {
                type: "CSV",
                fileName: fileName,
                rowCount: rowCount,
                columns: columns,
                processingTimeMs: Date.now() - startTime
              }
            });
            jobLogger.info("CSV processing complete", { rowCount, processingTime: Date.now() - startTime });
            resolve();
          })
          .on("error", reject);
      });
    }
    
    // PDF Processing with Gemini AI
    else if (fileName.endsWith(".pdf")) {
      jobLogger.info("PDF processing with Gemini AI");
      
      const buffer = fs.readFileSync(filePath);
      const pdfData = await pdfParse(buffer);
      const rawText = pdfData.text;
      
      let aiResult;
      let useFallback = false;
      
      try {
        aiResult = await parseWithGemini(rawText, fileName);
        jobLogger.info("Gemini AI parsing successful", { 
          documentType: aiResult.documentType,
          confidence: aiResult.confidence 
        });
      } catch (aiError) {
        jobLogger.warn("Gemini failed, using fallback", { error: aiError.message });
        useFallback = true;
        aiResult = basicParse(rawText, fileName);
      }
      
      const cleanText = rawText.replace(/\s+/g, ' ').trim();
      const wordCount = cleanText.split(/\s+/).length;
      
      const result = {
        type: "PDF",
        fileName: fileName,
        pageCount: pdfData.numpages,
        wordCount: wordCount,
        characterCount: rawText.length,
        processingTimeMs: Date.now() - startTime,
        parsedWith: useFallback ? "fallback" : "gemini-ai",
        documentType: aiResult.documentType || "Unknown",
        confidence: aiResult.confidence || "low",
        summary: aiResult.summary || cleanText.substring(0, 200),
        mainSections: aiResult.mainSections || [],
        keyTopics: aiResult.keyTopics || [],
        sentiment: aiResult.sentiment || "neutral",
        keyInformation: aiResult.keyInformation || {},
        preview: cleanText.substring(0, 500),
        metadata: {
          author: pdfData.info?.Author || "Unknown",
          creator: pdfData.info?.Creator || "Unknown",
          producer: pdfData.info?.Producer || "Unknown",
          creationDate: pdfData.info?.CreationDate || "Unknown"
        }
      };
      
      if (useFallback && aiResult.emails) {
        result.extractedEmails = aiResult.emails;
        result.extractedUrls = aiResult.urls;
      }
      
      await db.collection("jobs").doc(jobId).update({
        status: "completed",
        completedAt: new Date(),
        result: result
      });
      
      jobLogger.info("PDF processing complete", { 
        documentType: result.documentType, 
        wordCount: result.wordCount,
        processingTime: Date.now() - startTime
      });
    }

    totalProcessed++;
    message.ack();
    jobLogger.info("Job completed and acknowledged");

  } catch (err) {
    totalFailed++;
    jobLogger.error("Processing failed", { error: err.message, stack: err.stack });
    
    await db.collection("jobs").doc(jobId).update({
      status: "failed",
      failedAt: new Date(),
      result: { 
        error: err.message,
        processingTimeMs: Date.now() - startTime
      }
    });
    
    message.ack();
  } finally {
    // Cleanup file
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      jobLogger.debug("File cleaned up", { fileName });
    }
    
    activeProcesses--;
    processNext();
    
    // Log statistics every 10 jobs
    if ((totalProcessed + totalFailed) % 10 === 0) {
      const elapsed = (Date.now() - startTime) / 1000;
      logger.info("Batch worker statistics", {
        totalProcessed,
        totalFailed,
        activeProcesses,
        queueLength: queue.length,
        avgJobsPerSecond: ((totalProcessed + totalFailed) / elapsed).toFixed(2),
        uptimeSeconds: elapsed.toFixed(0)
      });
    }
  }
}

// ============================================
// QUEUE PROCESSOR
// ============================================
function processNext() {
  if (queue.length > 0 && activeProcesses < MAX_CONCURRENT) {
    const next = queue.shift();
    logger.debug("Queue processing", { 
      remainingQueue: queue.length, 
      activeProcesses: activeProcesses + 1 
    });
    processFile(next.jobId, next.filePath, next.fileName, next.message, next.batchId);
  }
}

// ============================================
// MESSAGE HANDLER WITH QUEUING
// ============================================
subscription.on("message", async (message) => {
  const data = JSON.parse(message.data.toString());
  const { jobId, filePath, fileName, batchId } = data;

  if (activeProcesses >= MAX_CONCURRENT) {
    if (queue.length < QUEUE_SIZE) {
      queue.push({ jobId, filePath, fileName, message, batchId });
      logger.info("Job queued", { 
        jobId, 
        fileName, 
        queuePosition: queue.length,
        activeProcesses 
      });
    } else {
      // Queue is full, reject this message
      logger.warn("Queue full, rejecting job", { jobId, queueSize: QUEUE_SIZE });
      await db.collection("jobs").doc(jobId).update({
        status: "failed",
        result: { error: "System busy. Queue full. Please try again later." }
      });
      message.ack();
    }
  } else {
    processFile(jobId, filePath, fileName, message, batchId);
  }
});

subscription.on("error", (error) => {
  logger.error("Subscription error", { error: error.message });
});

// ============================================
// STATISTICS ENDPOINT (via HTTP)
// ============================================
const express = require("express");
const statsApp = express();
const STATS_PORT = parseInt(process.env.STATS_PORT) || 3001;

statsApp.get("/stats", (req, res) => {
  const elapsed = (Date.now() - startTime) / 1000;
  res.json({
    status: "healthy",
    uptimeSeconds: elapsed,
    activeProcesses: activeProcesses,
    queueLength: queue.length,
    maxConcurrent: MAX_CONCURRENT,
    queueSize: QUEUE_SIZE,
    totalProcessed: totalProcessed,
    totalFailed: totalFailed,
    successRate: totalProcessed + totalFailed > 0 
      ? ((totalProcessed / (totalProcessed + totalFailed)) * 100).toFixed(2) 
      : 0,
    avgJobsPerSecond: ((totalProcessed + totalFailed) / elapsed).toFixed(2)
  });
});

statsApp.get("/stats/queue", (req, res) => {
  res.json({
    queueLength: queue.length,
    activeProcesses: activeProcesses,
    pendingQueue: queue.map(j => ({ jobId: j.jobId, fileName: j.fileName }))
  });
});

statsApp.listen(STATS_PORT, () => {
  logger.info("Stats server started", { port: STATS_PORT });
});

// ============================================
// GRACEFUL SHUTDOWN
// ============================================
let isShuttingDown = false;

async function gracefulShutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  
  logger.info("Received shutdown signal, closing gracefully...");
  
  // Wait for active processes to complete (max 30 seconds)
  let waitTime = 0;
  while (activeProcesses > 0 && waitTime < 30000) {
    logger.info("Waiting for active processes to complete", { 
      activeProcesses, 
      waitSeconds: waitTime / 1000 
    });
    await new Promise(resolve => setTimeout(resolve, 1000));
    waitTime += 1000;
  }
  
  if (activeProcesses > 0) {
    logger.warn("Force shutting down with active processes", { activeProcesses });
  }
  
  try {
    await subscription.close();
    logger.info("Subscription closed");
  } catch (err) {
    logger.error("Error closing subscription", { error: err.message });
  }
  
  try {
    await db.terminate();
    logger.info("Firestore connection closed");
  } catch (err) {
    logger.error("Error closing Firestore", { error: err.message });
  }
  
  const elapsed = (Date.now() - startTime) / 1000;
  logger.info("Graceful shutdown complete", {
    totalProcessed,
    totalFailed,
    uptimeSeconds: elapsed.toFixed(0),
    avgJobsPerSecond: ((totalProcessed + totalFailed) / elapsed).toFixed(2)
  });
  
  process.exit(0);
}

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

// ============================================
// STARTUP
// ============================================
logger.info("Batch worker started", { 
  maxConcurrent: MAX_CONCURRENT,
  queueSize: QUEUE_SIZE,
  batchSize: BATCH_SIZE,
  subscription: subscriptionName,
  geminiEnabled: true,
  statsPort: STATS_PORT
});

console.log(`
╔══════════════════════════════════════════════════════════════╗
║     🚀 BATCH WORKER IS RUNNING                               ║
╠══════════════════════════════════════════════════════════════╣
║  Subscription:   ${subscriptionName.padEnd(35)}║
║  Max Concurrent: ${String(MAX_CONCURRENT).padEnd(35)}║
║  Queue Size:     ${String(QUEUE_SIZE).padEnd(35)}║
║  Gemini AI:      Enabled${" ".padEnd(28)}║
║  Stats Port:     ${String(STATS_PORT).padEnd(35)}║
╠══════════════════════════════════════════════════════════════╣
║  Stats URL:      http://localhost:${STATS_PORT}/stats           ║
║  Queue Stats:    http://localhost:${STATS_PORT}/stats/queue     ║
╚══════════════════════════════════════════════════════════════╝
`);