const { PubSub } = require("@google-cloud/pubsub");
const admin = require("firebase-admin");
const fs = require("fs");
const csv = require("csv-parser");
const pdfParse = require("pdf-parse");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const { logger, createJobLogger, measureAsync } = require("./logger");
const https = require('https');
const http = require('http');
const Brevo = require('@getbrevo/brevo');
require("dotenv").config();

// ============================================
// CONFIGURATION
// ============================================
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 3;
const RETRY_DELAYS = [1000, 5000, 15000]; // 1s, 5s, 15s backoff
const MAX_FILE_SIZE = parseInt(process.env.MAX_FILE_SIZE) || 10 * 1024 * 1024; // 10MB

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

// Initialize PubSub with your project
const pubsub = new PubSub({
  projectId: "smpi-3f14b",
  credentials: serviceAccount
});

const subscriptionName = "file-processing-sub";
const subscription = pubsub.subscription(subscriptionName);

// ============================================
// WEBHOOK NOTIFICATION
// ============================================
async function sendWebhook(jobId, result, webhookUrl, status = "completed") {
  if (!webhookUrl) return;
  
  try {
    const url = new URL(webhookUrl);
    const client = url.protocol === 'https:' ? https : http;
    
    const payload = JSON.stringify({
      event: `job.${status}`,
      jobId: jobId,
      status: status,
      result: result,
      timestamp: new Date().toISOString(),
      environment: process.env.NODE_ENV || "development"
    });
    
    const options = {
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      },
      timeout: 5000
    };
    
    return new Promise((resolve) => {
      const req = client.request(options, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          logger.info("Webhook sent", { jobId, statusCode: res.statusCode, webhookUrl });
          resolve({ success: res.statusCode >= 200 && res.statusCode < 300 });
        });
      });
      
      req.on('error', (err) => {
        logger.error("Webhook failed", { jobId, error: err.message, webhookUrl });
        resolve({ success: false, error: err.message });
      });
      
      req.on('timeout', () => {
        req.destroy();
        logger.error("Webhook timeout", { jobId, webhookUrl });
        resolve({ success: false, error: "Timeout" });
      });
      
      req.write(payload);
      req.end();
    });
  } catch (err) {
    logger.error("Webhook error", { jobId, error: err.message });
    return { success: false, error: err.message };
  }
}

// ============================================
// EMAIL ALERTS
// ============================================
let brevoApiInstance = null;

function getBrevoClient() {
  if (!brevoApiInstance && process.env.BREVO_API_KEY) {
    brevoApiInstance = new Brevo.TransactionalEmailsApi();
    brevoApiInstance.setApiKey(Brevo.TransactionalEmailsApiApiKeys.apiKey, process.env.BREVO_API_KEY);
  }
  return brevoApiInstance;
}

async function sendFailureAlert(jobId, fileName, error, batchId = null) {
  const apiInstance = getBrevoClient();
  if (!apiInstance) {
    logger.warn("Brevo not configured, skipping email alert", { jobId });
    return;
  }
  
  try {
    const sendSmtpEmail = new Brevo.SendSmtpEmail();
    sendSmtpEmail.subject = `❌ Job Failed: ${jobId.substring(0, 8)}`;
    sendSmtpEmail.to = [{ email: process.env.ALERT_EMAIL }];
    sendSmtpEmail.htmlContent = `
      <!DOCTYPE html>
      <html>
      <head>
        <style>
          body { font-family: Arial, sans-serif; }
          .container { padding: 20px; max-width: 600px; margin: 0 auto; }
          .header { background: #dc3545; color: white; padding: 10px; text-align: center; }
          .content { border: 1px solid #ddd; padding: 20px; }
          .label { font-weight: bold; color: #333; }
          .error { background: #f8d7da; padding: 10px; border-left: 4px solid #dc3545; }
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h2>⚠️ Document Processing Failed</h2>
          </div>
          <div class="content">
            <p><span class="label">Job ID:</span> ${jobId}</p>
            <p><span class="label">File Name:</span> ${fileName}</p>
            ${batchId ? `<p><span class="label">Batch ID:</span> ${batchId}</p>` : ''}
            <p><span class="label">Time:</span> ${new Date().toISOString()}</p>
            <div class="error">
              <p><span class="label">Error:</span></p>
              <p>${error}</p>
            </div>
            <p><small>This is an automated alert from your Document Processing System.</small></p>
          </div>
        </div>
      </body>
      </html>
    `;
    sendSmtpEmail.sender = { 
      email: "alerts@document-processor.com", 
      name: "Document Processor System" 
    };
    
    await apiInstance.sendTransacEmail(sendSmtpEmail);
    logger.info("Email alert sent", { jobId });
  } catch (err) {
    logger.error("Email alert failed", { jobId, error: err.message });
  }
}

// ============================================
// GEMINI AI PARSING FUNCTION (Enhanced)
// ============================================
async function parseWithGemini(text, fileName, retryCount = 0) {
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
    if (retryCount < MAX_RETRIES) {
      const delay = RETRY_DELAYS[retryCount];
      logger.warn("Gemini parsing failed, retrying", { 
        error: err.message, 
        fileName, 
        retry: retryCount + 1,
        delayMs: delay 
      });
      await new Promise(resolve => setTimeout(resolve, delay));
      return parseWithGemini(text, fileName, retryCount + 1);
    }
    
    logger.error("Gemini parsing error after retries", { error: err.message, fileName });
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
// ENHANCED BASIC FALLBACK PARSING
// ============================================
function basicParse(text, fileName) {
  const cleanText = text.replace(/\s+/g, ' ').trim();
  const wordCount = cleanText.split(/\s+/).length;
  
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const urlRegex = /https?:\/\/[^\s]+/g;
  const phoneRegex = /(\+\d{1,3}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}/g;
  const nameRegex = /(?:name|full name)[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/i;
  const orgRegex = /(?:company|organization|university)[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/i;
  
  const emails = cleanText.match(emailRegex) || [];
  const urls = cleanText.match(urlRegex) || [];
  const phones = cleanText.match(phoneRegex) || [];
  const nameMatch = cleanText.match(nameRegex);
  const orgMatch = cleanText.match(orgRegex);
  
  let detectedType = "Unknown";
  const textLower = cleanText.toLowerCase();
  if (textLower.includes("resume") || textLower.includes("cv") || textLower.includes("curriculum vitae")) {
    detectedType = "Resume";
  } else if (textLower.includes("invoice") || textLower.includes("bill")) {
    detectedType = "Invoice";
  } else if (textLower.includes("report")) {
    detectedType = "Report";
  } else if (textLower.includes("contract") || textLower.includes("agreement")) {
    detectedType = "Contract";
  }
  
  return {
    documentType: detectedType,
    confidence: "low",
    keyInformation: {
      name: nameMatch ? nameMatch[1] : null,
      email: emails[0] || null,
      phone: phones[0] || null,
      organization: orgMatch ? orgMatch[1] : null,
      url: urls[0] || null
    },
    summary: cleanText.substring(0, 200),
    mainSections: [],
    keyTopics: [],
    sentiment: "neutral",
    wordCount: wordCount,
    emails: emails.slice(0, 5),
    urls: urls.slice(0, 5),
    phones: phones.slice(0, 3)
  };
}

// ============================================
// FILE VALIDATION
// ============================================
function validateFile(filePath, fileName) {
  const stats = fs.statSync(filePath);
  if (stats.size > MAX_FILE_SIZE) {
    throw new Error(`File size (${stats.size} bytes) exceeds limit (${MAX_FILE_SIZE} bytes)`);
  }
  
  const ext = fileName.split('.').pop().toLowerCase();
  if (!['pdf', 'csv'].includes(ext)) {
    throw new Error(`Unsupported file type: ${ext}`);
  }
  
  try {
    fs.accessSync(filePath, fs.constants.R_OK);
  } catch (err) {
    throw new Error(`File not readable: ${err.message}`);
  }
  
  return true;
}

// Helper function for safe acknowledgment
async function safeAck(message, jobId, filePath, fileName, jobLogger) {
  try {
    message.ack();
    jobLogger.info("Message acknowledged", { jobId });
    
    if (filePath && fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      jobLogger.info("File cleaned up", { fileName });
    }
  } catch (err) {
    jobLogger.error("Failed to acknowledge", { error: err.message });
  }
}

// ============================================
// PROCESSING WITH RETRY LOGIC & PROGRESS
// ============================================
async function processWithRetry(jobId, filePath, fileName, jobLogger, retryCount = 0, jobData = {}) {
  try {
    validateFile(filePath, fileName);
    
    // CSV Processing with Progress Tracking
    if (fileName.endsWith(".csv")) {
      jobLogger.info("Starting CSV processing");
      
      // First pass: count total rows
      let totalRows = 0;
      await new Promise((resolve) => {
        fs.createReadStream(filePath)
          .pipe(csv())
          .on("data", () => totalRows++)
          .on("end", resolve);
      });
      
      await db.collection("jobs").doc(jobId).update({
        progress: 5,
        statusMessage: `Found ${totalRows} rows to process`
      });
      
      return await new Promise((resolve, reject) => {
        let rowCount = 0;
        let columns = [];
        let sampleRows = [];
        let processedRows = 0;
        let lastProgressUpdate = 0;

        fs.createReadStream(filePath)
          .pipe(csv())
          .on("headers", (headers) => {
            columns = headers;
            jobLogger.debug("CSV headers detected", { columnCount: headers.length });
          })
          .on("data", async (row) => {
            rowCount++;
            processedRows++;
            if (sampleRows.length < 5) {
              sampleRows.push(row);
            }
            
            // Update progress every 10%
            const percent = Math.floor((processedRows / totalRows) * 100);
            if (percent >= lastProgressUpdate + 10 && percent > 0) {
              lastProgressUpdate = percent;
              await db.collection("jobs").doc(jobId).update({
                progress: Math.min(percent, 95),
                statusMessage: `Processing row ${processedRows}/${totalRows} (${percent}%)`
              }).catch(() => {});
            }
          })
          .on("end", () => {
            resolve({
              type: "CSV",
              fileName: fileName,
              rowCount: rowCount,
              columns: columns,
              sampleRows: sampleRows,
              summary: `CSV file with ${rowCount} rows and ${columns.length} columns`
            });
          })
          .on("error", reject);
      });
    }
    
    // PDF Processing with Gemini AI & Progress Tracking
    else if (fileName.endsWith(".pdf")) {
      jobLogger.info("Starting PDF processing with Gemini AI");
      
      await db.collection("jobs").doc(jobId).update({
        progress: 10,
        statusMessage: "Reading PDF file..."
      });
      
      const buffer = fs.readFileSync(filePath);
      
      await db.collection("jobs").doc(jobId).update({
        progress: 30,
        statusMessage: "Extracting text from PDF..."
      });
      
      const pdfData = await pdfParse(buffer);
      const rawText = pdfData.text;
      
      if (rawText.length < 10) {
        throw new Error("PDF contains no extractable text");
      }
      
      await db.collection("jobs").doc(jobId).update({
        progress: 50,
        statusMessage: "Analyzing document with AI..."
      });
      
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
      
      await db.collection("jobs").doc(jobId).update({
        progress: 80,
        statusMessage: "Preparing results..."
      });
      
      const cleanText = rawText.replace(/\s+/g, ' ').trim();
      const wordCount = cleanText.split(/\s+/).length;
      
      const result = {
        type: "PDF",
        fileName: fileName,
        pageCount: pdfData.numpages,
        wordCount: wordCount,
        characterCount: rawText.length,
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
        },
        ...(useFallback && aiResult.emails && { extractedEmails: aiResult.emails }),
        ...(useFallback && aiResult.urls && { extractedUrls: aiResult.urls })
      };
      
      return result;
    }
    
    throw new Error(`Unsupported file type: ${fileName}`);
    
  } catch (err) {
    if (retryCount < MAX_RETRIES) {
      const delay = RETRY_DELAYS[retryCount];
      jobLogger.warn(`Processing failed, retrying (${retryCount + 1}/${MAX_RETRIES})`, { 
        error: err.message,
        delayMs: delay 
      });
      await new Promise(resolve => setTimeout(resolve, delay));
      return processWithRetry(jobId, filePath, fileName, jobLogger, retryCount + 1, jobData);
    }
    throw err;
  }
}

// ============================================
// MAIN MESSAGE HANDLER
// ============================================
subscription.on("message", async (message) => {
  const data = JSON.parse(message.data.toString());
  const { jobId, filePath, fileName, batchId, webhookUrl } = data;
  
  // Fetch job data from Firestore to get webhookUrl and other fields
  const jobDoc = await db.collection("jobs").doc(jobId).get();
  const jobData = jobDoc.exists ? jobDoc.data() : { webhookUrl };
  
  const jobLogger = createJobLogger(jobId);
  jobLogger.info("Job received", { fileName, filePath, batchId: batchId || "none" });

  const timeoutId = setTimeout(async () => {
    jobLogger.error("Job timeout", { timeout: "5 minutes" });
    await db.collection("jobs").doc(jobId).update({
      status: "failed",
      failedAt: new Date(),
      progress: 0,
      statusMessage: "Processing timeout",
      result: { error: "Processing timeout (5 minutes)" }
    });
    message.ack();
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    
    // Send failure webhook
    if (jobData.webhookUrl) {
      await sendWebhook(jobId, { error: "Processing timeout" }, jobData.webhookUrl, "failed");
    }
  }, 5 * 60 * 1000);

  try {
    await db.collection("jobs").doc(jobId).update({
      status: "processing",
      processingStartedAt: new Date(),
      progress: 0,
      statusMessage: "Starting processing...",
      ...(batchId && { batchId })
    });
    jobLogger.info("Status updated to processing");

    const startTime = Date.now();
    const result = await processWithRetry(jobId, filePath, fileName, jobLogger, 0, jobData);
    const processingTime = Date.now() - startTime;
    
    result.processingTimeMs = processingTime;
    
    await db.collection("jobs").doc(jobId).update({
      status: "completed",
      completedAt: new Date(),
      progress: 100,
      statusMessage: "Processing complete",
      result: result
    });
    
    jobLogger.info("Processing complete", { 
      documentType: result.documentType || result.type,
      processingTimeMs: processingTime,
      ...(result.wordCount && { wordCount: result.wordCount })
    });
    
    clearTimeout(timeoutId);
    
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      jobLogger.debug("File deleted", { fileName });
    }
    
    message.ack();
    jobLogger.info("Message acknowledged");
    
    // Send success webhook
    if (jobData.webhookUrl) {
      await sendWebhook(jobId, result, jobData.webhookUrl, "completed");
    }
    
  } catch (err) {
    clearTimeout(timeoutId);
    jobLogger.error("Worker error", { error: err.message, stack: err.stack });
    
    await db.collection("jobs").doc(jobId).update({
      status: "failed",
      failedAt: new Date(),
      progress: 0,
      statusMessage: `Failed: ${err.message.substring(0, 100)}`,
      result: { 
        error: err.message,
        processingAttempts: MAX_RETRIES
      }
    });
    
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      jobLogger.debug("Failed file cleaned up", { fileName });
    }
    message.ack();
    
    // Send failure webhook
    if (jobData.webhookUrl) {
      await sendWebhook(jobId, { error: err.message }, jobData.webhookUrl, "failed");
    }
    
    // Send email alert
    await sendFailureAlert(jobId, fileName, err.message, batchId);
  }
});

subscription.on("error", (error) => {
  logger.error("Subscription error", { error: error.message });
});

// ============================================
// HEALTH CHECK FOR WORKER
// ============================================
const express = require("express");
const healthApp = express();
const HEALTH_PORT = parseInt(process.env.WORKER_HEALTH_PORT) || 3002;

healthApp.get("/health", (req, res) => {
  res.json({
    status: "healthy",
    service: "worker",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    config: {
      maxRetries: MAX_RETRIES,
      maxFileSize: MAX_FILE_SIZE,
      geminiEnabled: true
    }
  });
});

healthApp.get("/ready", (req, res) => {
  res.json({ ready: true });
});

healthApp.listen(HEALTH_PORT, () => {
  logger.info("Worker health server started", { port: HEALTH_PORT });
});

// ============================================
// GRACEFUL SHUTDOWN
// ============================================
let isShuttingDown = false;

async function gracefulShutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  
  logger.info("Received shutdown signal, closing gracefully...");
  
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
  
  logger.info("Graceful shutdown complete");
  process.exit(0);
}

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

// ============================================
// STARTUP
// ============================================
logger.info("Worker started", { 
  subscription: subscriptionName,
  geminiEnabled: true,
  projectId: "smpi-3f14b",
  maxRetries: MAX_RETRIES,
  maxFileSize: MAX_FILE_SIZE,
  healthPort: HEALTH_PORT,
  brevoEnabled: !!process.env.BREVO_API_KEY,
  webhookEnabled: true
});

console.log(`
╔══════════════════════════════════════════════════════════════════════════╗
║     🤖 WORKER IS RUNNING (FULLY UPGRADED)                                ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Subscription:   ${subscriptionName.padEnd(40)}║
║  Gemini AI:      Enabled${" ".padEnd(33)}║
║  Max Retries:    ${String(MAX_RETRIES).padEnd(40)}║
║  Max File Size:  ${String(MAX_FILE_SIZE / (1024 * 1024)).padEnd(40)}MB║
║  Logging:        Winston (logs/ folder)${" ".padEnd(18)}║
║  Health Port:    ${String(HEALTH_PORT).padEnd(40)}║
║  Email Alerts:   ${process.env.BREVO_API_KEY ? "Enabled".padEnd(40) : "Disabled (no API key)".padEnd(40)}║
║  Webhooks:       Enabled${" ".padEnd(33)}║
╠══════════════════════════════════════════════════════════════════════════╣
║  Health Check:   http://localhost:${HEALTH_PORT}/health                          ║
║  Readiness:      http://localhost:${HEALTH_PORT}/ready                            ║
╚══════════════════════════════════════════════════════════════════════════╝
`);