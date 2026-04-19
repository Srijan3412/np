const { PubSub } = require("@google-cloud/pubsub");
const admin = require("firebase-admin");
const fs = require("fs");
const csv = require("csv-parser");
const pdfParse = require("pdf-parse");

const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

const pubsub = new PubSub();
const subscription = pubsub.subscription("file-processing-sub");

// Listen for messages
subscription.on("message", async (message) => {
  const data = JSON.parse(message.data.toString());
  const { jobId, filePath, fileName } = data;

  console.log("📥 Received job:", jobId);

  try {
    await db.collection("jobs").doc(jobId).update({
      status: "processing"
    });

    // CSV
    if (fileName.endsWith(".csv")) {
      let rowCount = 0;
      let columns = [];

      fs.createReadStream(filePath)
        .pipe(csv())
        .on("headers", (headers) => {
          columns = headers;
        })
        .on("data", () => rowCount++)
        .on("end", async () => {
          await db.collection("jobs").doc(jobId).update({
            status: "completed",
            result: { type: "CSV", rowCount, columns }
          });

          console.log("✅ CSV done:", jobId);
        });
    }

    // PDF
    else if (fileName.endsWith(".pdf")) {
      const buffer = fs.readFileSync(filePath);
      const data = await pdfParse(buffer);

      const wordCount = data.text.split(/\s+/).length;

      await db.collection("jobs").doc(jobId).update({
        status: "completed",
        result: {
          type: "PDF",
          wordCount,
          preview: data.text.substring(0, 200)
        }
      });

      console.log("✅ PDF done:", jobId);
    }

  } catch (err) {
    await db.collection("jobs").doc(jobId).update({
      status: "failed",
      result: { error: err.message }
    });

    console.error("❌ Worker error:", err);
  }

  message.ack();
});

console.log("🚀 Worker listening...");