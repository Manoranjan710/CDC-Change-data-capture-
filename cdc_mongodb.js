/**
 * cdc_mongodb.js
 *
 * MongoDB Change Data Capture -> Redis Streams
 */

const { MongoClient } = require("mongodb");
const { createClient } = require("redis");
const { EJSON } = require("bson");
const fs = require("fs").promises;
const path = require("path");

// Environment variables / defaults
const MONGO_URI = process.env.MONGO_URI || "mongodb://localhost:27017";
const MONGO_DB = process.env.MONGO_DB || "cloth_store";
const MONGO_COLLECTION = process.env.MONGO_COLLECTION || "shirts";

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";
const REDIS_STREAM = process.env.REDIS_STREAM || "cloth_store_changes";

const RESUME_TOKEN_FILE = path.resolve(__dirname, ".mongo_resume_token.ejson");

let shuttingDown = false;

/**
 * Resume token load/save
 */
async function loadResumeToken() {
  try {
    const data = await fs.readFile(RESUME_TOKEN_FILE, "utf8");
    return EJSON.parse(data);
  } catch (err) {
    return null;
  }
}

async function persistResumeToken(token) {
  try {
    if (token) {
      await fs.writeFile(RESUME_TOKEN_FILE, EJSON.stringify(token), "utf8");
    }
  } catch (err) {
    console.error("Failed to persist resume token:", err);
  }
}

/**
 * Normalize the change event
 */
function normalizeChange(change, dbName, collName) {
  return {
    db: dbName,
    collection: collName,
    operationType: change.operationType,
    documentKey: change.documentKey || null,
    fullDocument: change.fullDocument || null,
    updateDescription: change.updateDescription || null,
    clusterTime: change.clusterTime ? change.clusterTime.toString() : null,
    resumeToken: change._id || null,
    timestamp: new Date().toISOString(),
  };
}

async function main() {
  // Redis
  const redis = createClient({ url: REDIS_URL });
  redis.on("error", (err) => console.error("Redis error:", err));
  await redis.connect();

  // MongoDB
  const mongo = new MongoClient(MONGO_URI);
  await mongo.connect();
  const db = mongo.db(MONGO_DB);
  const collection = db.collection(MONGO_COLLECTION);

  console.log(`Connected to MongoDB: ${MONGO_URI}/${MONGO_DB}.${MONGO_COLLECTION}`);
  console.log(`Connected to Redis: ${REDIS_URL} -> stream "${REDIS_STREAM}"`);

  // Resume token
  const resumeToken = await loadResumeToken();
  const watchOptions = { fullDocument: "updateLookup" };

  if (resumeToken) {
    console.log("Resuming from token:", resumeToken);
    watchOptions.resumeAfter = resumeToken;
  }

  const changeStream = collection.watch([], watchOptions);

  changeStream.on("change", async (change) => {
    if (shuttingDown) return;

    try {
      const payload = normalizeChange(change, MONGO_DB, MONGO_COLLECTION);
      // Use EJSON.stringify (strict mode) so Mongo types like ObjectId are emitted as {"$oid": "..."}
      const message = EJSON.stringify(payload, { relaxed: false });

      await redis.xAdd(REDIS_STREAM, "*", { change: message });

      await persistResumeToken(change._id);

      // Log the full payload as extended JSON for clarity
      console.log(`CDC Event: ${payload.operationType}`, EJSON.stringify(payload, { relaxed: false }));
    } catch (err) {
      console.error("Error processing change:", err);
    }
  });

  changeStream.on("error", (err) => {
    console.error("Change Stream Error:", err);
  });

  /** Graceful shutdown */
  async function shutdown() {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log("Shutting down CDC worker...");

    try {
      await changeStream.close();
    } catch {}

    try {
      await mongo.close();
    } catch {}

    try {
      await redis.disconnect();
    } catch {}

    process.exit(0);
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

// Start
main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
