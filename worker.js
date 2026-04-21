const { PubSub } = require("@google-cloud/pubsub");
const admin = require("firebase-admin");
const fs = require("fs");
const csv = require("csv-parser");
const pdfParse = require("pdf-parse");

// Set your project ID
process.env.GOOGLE_CLOUD_PROJECT = "smpi-3f14b";

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

// Use your exact subscription name
const subscriptionName = "file-processing-sub";
const subscription = pubsub.subscription(subscriptionName);

// Helper function to safely acknowledge messages
async function safeAck(message, jobId, filePath, fileName) {
  try {
    message.ack();
    console.log(`✅ Acknowledged: ${jobId}`);
    
    // Clean up file if not already deleted
    if (filePath && fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      console.log(`🗑️ Cleaned up: ${fileName}`);
    }
  } catch (err) {
    console.error(`Failed to ack ${jobId}:`, err.message);
  }
}

// Listen for messages with TIMEOUT HANDLING
subscription.on("message", async (message) => {
  const data = JSON.parse(message.data.toString());
  const { jobId, filePath, fileName } = data;

  console.log("📥 Received job:", jobId);

  // ✅ Add timeout (5 minutes max processing time)
  const timeoutId = setTimeout(async () => {
    console.error(`⏰ Timeout for job: ${jobId}`);
    await db.collection("jobs").doc(jobId).update({
      status: "failed",
      result: { error: "Processing timeout (5 minutes)" }
    });
    message.ack();
    
    // Cleanup file
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      console.log(`🗑️ Cleaned up timeout file: ${fileName}`);
    }
  }, 5 * 60 * 1000);  // 5 minutes

  try {
    await db.collection("jobs").doc(jobId).update({
      status: "processing"
    });

    // CSV Processing with safe ack
    if (fileName.endsWith(".csv")) {
      let rowCount = 0;
      let columns = [];
      let processed = false;

      const stream = fs.createReadStream(filePath)
        .pipe(csv())
        .on("headers", (headers) => {
          columns = headers;
        })
        .on("data", () => rowCount++)
        .on("end", async () => {
          if (processed) return;
          processed = true;
          
          await db.collection("jobs").doc(jobId).update({
            status: "completed",
            result: { type: "CSV", rowCount, columns }
          });
          console.log("✅ CSV done:", jobId);
          
          clearTimeout(timeoutId);
          await safeAck(message, jobId, filePath, fileName);
        })
        .on("error", async (err) => {
          if (processed) return;
          processed = true;
          
          await db.collection("jobs").doc(jobId).update({
            status: "failed",
            result: { error: err.message }
          });
          console.error("❌ CSV error:", err);
          clearTimeout(timeoutId);
          await safeAck(message, jobId, filePath, fileName);
        });
    }
    // PDF Processing with ULTIMATE extraction
    else if (fileName.endsWith(".pdf")) {
      const buffer = fs.readFileSync(filePath);
      const data = await pdfParse(buffer);
      
      // ============================================
      // 1. TEXT CLEANING & NORMALIZATION
      // ============================================
      const rawText = data.text;
      
      // Remove excessive whitespace
      const cleanText = rawText
        .replace(/\r\n/g, '\n')
        .replace(/\s+/g, ' ')
        .replace(/\n\s+\n/g, '\n\n')
        .replace(/[^\x20-\x7E\n]/g, '')
        .trim();
      
      // Preserve paragraph structure
      const paragraphs = rawText
        .split(/\n\s*\n/)
        .map(p => p.replace(/\s+/g, ' ').trim())
        .filter(p => p.length > 50);
      
      const wordCount = cleanText.split(/\s+/).filter(w => w.length > 0).length;
      const sentenceCount = cleanText.split(/[.!?]+/).filter(s => s.trim().length > 0).length;
      
      // ============================================
      // 2. COMPLETE METADATA EXTRACTION
      // ============================================
      const metadata = {
        title: data.info?.Title || "Untitled",
        author: data.info?.Author || "Unknown Author",
        subject: data.info?.Subject || "No subject",
        keywords: data.info?.Keywords || "None",
        creator: data.info?.Creator || "Unknown",
        producer: data.info?.Producer || "Unknown",
        creationDate: data.info?.CreationDate || "Unknown",
        modificationDate: data.info?.ModDate || "Unknown",
        trapped: data.info?.Trapped || "Unknown",
        encrypted: data.info?.Encrypted || false
      };
      
      // Parse dates if present
      const parseDate = (dateStr) => {
        if (dateStr === "Unknown") return null;
        try {
          const match = dateStr.match(/D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/);
          if (match) {
            return new Date(Date.UTC(match[1], match[2]-1, match[3], match[4], match[5], match[6]));
          }
          return null;
        } catch {
          return null;
        }
      };
      
      metadata.parsedCreationDate = parseDate(metadata.creationDate);
      metadata.parsedModificationDate = parseDate(metadata.modificationDate);
      
      // ============================================
      // 3. INTELLIGENT DOCUMENT CLASSIFICATION
      // ============================================
      const textLower = cleanText.toLowerCase();
      
      const documentTypes = {
        "Resume/CV": ["resume", "cv", "curriculum vitae", "work experience", "education", "skills"],
        "Invoice": ["invoice", "bill", "payment due", "amount due", "total"],
        "Report": ["report", "analysis", "findings", "conclusion", "recommendation"],
        "Contract": ["contract", "agreement", "terms", "conditions", "parties"],
        "Letter": ["dear", "sincerely", "regards", "letter"],
        "Scientific Paper": ["abstract", "introduction", "methodology", "results", "conclusion", "references"],
        "Legal Document": ["whereas", "hereinafter", "witnesseth", "affidavit"],
        "Financial": ["balance sheet", "income statement", "profit", "loss", "revenue"],
        "Educational": ["syllabus", "course", "assignment", "homework", "exam"]
      };
      
      let documentType = "General Document";
      let confidence = 0;
      
      for (const [type, keywords] of Object.entries(documentTypes)) {
        let score = 0;
        for (const keyword of keywords) {
          if (textLower.includes(keyword)) {
            score += 1;
          }
        }
        if (score > confidence) {
          confidence = score;
          documentType = type;
        }
      }
      
      // ============================================
      // 4. KEY PHRASE & ENTITY EXTRACTION
      // ============================================
      const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
      const emails = [...new Set(cleanText.match(emailRegex) || [])];
      
      const urlRegex = /https?:\/\/[^\s]+/g;
      const urls = [...new Set(cleanText.match(urlRegex) || [])];
      
      const phoneRegex = /(\+?\d{1,3}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}/g;
      const phones = [...new Set(cleanText.match(phoneRegex) || [])];
      
      const dateRegex = /\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b/gi;
      const dates = [...new Set(cleanText.match(dateRegex) || [])].slice(0, 10);
      
      const currencyRegex = /\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?|\d+(?:\.\d{2})?\s?(?:USD|EUR|GBP)/g;
      const currencies = [...new Set(cleanText.match(currencyRegex) || [])];
      
      const percentageRegex = /\d+(?:\.\d+)?%/g;
      const percentages = [...new Set(cleanText.match(percentageRegex) || [])];
      
      // ============================================
      // 5. SECTION DETECTION & EXTRACTION
      // ============================================
      const sections = {};
      const sectionPatterns = {
        summary: /(?:summary|profile|objective|about me)[:\s]*([^\n]+(?:\n[^\n]+){0,5})/i,
        education: /(?:education|academic|qualifications)[:\s]*([^\n]+(?:\n[^\n]+){0,10})/i,
        experience: /(?:experience|employment|work history|professional)[:\s]*([^\n]+(?:\n[^\n]+){0,15})/i,
        skills: /(?:skills|competencies|technologies|expertise)[:\s]*([^\n]+(?:\n[^\n]+){0,10})/i,
        certifications: /(?:certifications|certificates|licenses)[:\s]*([^\n]+(?:\n[^\n]+){0,10})/i,
        projects: /(?:projects|portfolio)[:\s]*([^\n]+(?:\n[^\n]+){0,15})/i,
        languages: /(?:languages|language proficiency)[:\s]*([^\n]+(?:\n[^\n]+){0,5})/i,
        references: /(?:references|referees)[:\s]*([^\n]+(?:\n[^\n]+){0,10})/i
      };
      
      for (const [section, pattern] of Object.entries(sectionPatterns)) {
        const match = rawText.match(pattern);
        if (match) {
          sections[section] = match[1].replace(/\s+/g, ' ').trim().substring(0, 300);
        }
      }
      
      // ============================================
      // 6. TEXT STATISTICS
      // ============================================
      const statistics = {
        wordCount: wordCount,
        characterCount: rawText.length,
        characterCountNoSpaces: rawText.replace(/\s/g, '').length,
        sentenceCount: sentenceCount,
        paragraphCount: paragraphs.length,
        averageWordLength: (rawText.replace(/\s/g, '').length / wordCount).toFixed(1),
        averageWordsPerSentence: (wordCount / sentenceCount).toFixed(1),
        estimatedReadingTimeMinutes: Math.ceil(wordCount / 200),
        uniqueWordCount: new Set(cleanText.toLowerCase().split(/\s+/)).size
      };
      
      // ============================================
      // 7. FREQUENT WORDS & KEYWORDS
      // ============================================
      const stopWords = new Set(['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'but', 'so', 'if', 'then', 'else', 'when', 'where', 'which', 'while', 'this', 'that', 'these', 'those', 'from', 'as', 'about', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'between', 'under', 'over', 'more', 'most', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'than', 'that', 'then', 'thence', 'there', 'these', 'they', 'this', 'those', 'through', 'throughout', 'thru', 'until', 'up', 'upon', 'with', 'within', 'without']);
      
      const words = cleanText.toLowerCase().split(/\s+/).filter(w => w.length > 3 && !stopWords.has(w));
      const wordFrequency = {};
      for (const word of words) {
        wordFrequency[word] = (wordFrequency[word] || 0) + 1;
      }
      
      const topKeywords = Object.entries(wordFrequency)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .map(([word, count]) => ({ word, count }));
      
      // ============================================
      // 8. LANGUAGE DETECTION
      // ============================================
      const detectLanguage = (text) => {
        const langPatterns = {
          english: /\b(the|and|of|to|in|for|is|on|that|by|this|with|are|from|was|at|be|have|has)\b/gi,
          spanish: /\b(el|la|de|y|que|en|un|se|por|con|no|una|para|los|del)\b/gi,
          french: /\b(le|la|de|et|les|des|un|une|est|pour|par|que|en|dans)\b/gi,
          german: /\b(der|die|und|den|des|mit|von|sie|das|ist|nicht|auf|für|sich|dem)\b/gi
        };
        
        let bestLang = "english";
        let maxMatches = 0;
        
        for (const [lang, pattern] of Object.entries(langPatterns)) {
          const matches = text.match(pattern) || [];
          if (matches.length > maxMatches) {
            maxMatches = matches.length;
            bestLang = lang;
          }
        }
        
        return bestLang;
      };
      
      const detectedLanguage = detectLanguage(cleanText);
      
      // ============================================
      // 9. FORMAT EXTRACTION
      // ============================================
      const hasBulletPoints = /[•·*\-]\s/.test(rawText);
      const hasNumberedList = /\d+\.\s/.test(rawText);
      const hasTables = /[+-]+\+/.test(rawText) || /\|\s*.+\s*\|/.test(rawText);
      const hasImages = /\.(jpg|jpeg|png|gif|bmp)/i.test(rawText);
      
      // ============================================
      // 10. FINAL RESULT ASSEMBLY
      // ============================================
      const firstLines = cleanText.split('\n').slice(0, 5).join(' ').substring(0, 250);
      const preview = cleanText.substring(0, 800);
      
      await db.collection("jobs").doc(jobId).update({
        status: "completed",
        result: {
          type: "PDF",
          fileName: fileName,
          pageCount: data.numpages,
          statistics: statistics,
          documentType: documentType,
          documentTypeConfidence: confidence,
          detectedLanguage: detectedLanguage,
          entities: {
            emails: emails.slice(0, 5),
            urls: urls.slice(0, 5),
            phones: phones.slice(0, 5),
            dates: dates,
            currencies: currencies.slice(0, 5),
            percentages: percentages.slice(0, 5)
          },
          sections: sections,
          topKeywords: topKeywords,
          format: {
            hasBulletPoints: hasBulletPoints,
            hasNumberedList: hasNumberedList,
            hasTables: hasTables,
            hasImages: hasImages
          },
          metadata: metadata,
          summary: firstLines,
          preview: preview,
          firstParagraph: paragraphs[0] || ""
        }
      });
      
      console.log(`✅ PDF processed: ${fileName} | ${wordCount} words | ${data.numpages} pages | Type: ${documentType}`);
      
      clearTimeout(timeoutId);
      
      // Delete the file
      try {
        fs.unlinkSync(filePath);
        console.log(`🗑️ Deleted: ${fileName}`);
      } catch (err) {
        console.error(`Failed to delete ${fileName}:`, err.message);
      }
      
      message.ack();
    }
  } catch (err) {
    clearTimeout(timeoutId);
    
    await db.collection("jobs").doc(jobId).update({
      status: "failed",
      result: { error: err.message }
    });
    console.error("❌ Worker error:", err);
    
    // Delete file even on failure
    try {
      if (filePath && fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        console.log(`🗑️ Deleted failed file: ${fileName}`);
      }
    } catch (cleanupErr) {
      console.error("Cleanup error:", cleanupErr.message);
    }
    
    message.ack();
  }
});

subscription.on("error", (error) => {
  console.error("❌ Subscription error:", error);
});

// ✅ GRACEFUL SHUTDOWN for WORKER.JS
let isShuttingDown = false;

async function gracefulShutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  
  console.log("\n🛑 Received shutdown signal. Closing gracefully...");
  
  try {
    await subscription.close();
    console.log("📡 Subscription closed");
  } catch (err) {
    console.error("Error closing subscription:", err.message);
  }
  
  try {
    await db.terminate();
    console.log("💾 Firestore connection closed");
  } catch (err) {
    console.error("Error closing Firestore:", err.message);
  }
  
  console.log("✅ Graceful shutdown complete");
  process.exit(0);
}

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

console.log("🚀 Worker listening on subscription: file-processing-sub");