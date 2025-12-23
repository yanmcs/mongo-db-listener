const { MongoClient } = require('mongodb');
const amqp = require('amqplib');
const http = require('http');

const mongoUri = process.env.MONGO_URI || 'mongodb://localhost:27017';
const dbName = process.env.DB_NAME || 'test';
const collectionNames = (process.env.COLLECTION_NAMES || 'changes').split(',').map(name => name.trim());
const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://localhost';
const queueName = process.env.QUEUE_NAME || 'mongo_changes';
const syncCollectionName = '_sync_state';
const healthPort = process.env.HEALTH_PORT || 3000;

const MAX_RETRY_DELAY = 5 * 60 * 1000; // 5 minutes in ms
const RABBITMQ_HEARTBEAT = 60; // seconds
const MONGO_HEARTBEAT_MS = 10000; // 10 seconds
const MONGO_SERVER_SELECTION_TIMEOUT_MS = 30000; // 30 seconds
// IMPORTANT for change streams:
// - `socketTimeoutMS` should usually be 0 (no timeout), otherwise an idle period can close the stream.
// - If you do set it, ensure it's comfortably larger than `maxAwaitTimeMS`.
const MONGO_SOCKET_TIMEOUT_MS = parseInt(process.env.MONGO_SOCKET_TIMEOUT_MS || '0', 10);
const MONGO_MAX_AWAIT_TIME_MS = parseInt(process.env.MONGO_MAX_AWAIT_TIME_MS || '60000', 10);
const RETRY_RESET_AFTER_MS = parseInt(process.env.RETRY_RESET_AFTER_MS || String(5 * 60 * 1000), 10); // default 5 minutes

// 🚦 Rate limiting & queue settings
const MAX_CONCURRENT_PROCESSING = parseInt(process.env.MAX_CONCURRENT || '10', 10);
const MAX_QUEUE_SIZE = parseInt(process.env.MAX_QUEUE_SIZE || '1000', 10);
const QUEUE_DRAIN_INTERVAL_MS = 100; // How often to check queue when paused

// 📊 Monitoring & maintenance settings
const STATUS_LOG_INTERVAL_MS = 5 * 60 * 1000; // Log status every 5 minutes
const HEALTH_CHECK_INTERVAL_MS = 60 * 1000; // Proactive health check every minute
const MAX_MEMORY_MB = parseInt(process.env.MAX_MEMORY_MB || '512', 10);
const COUNTER_RESET_THRESHOLD = Number.MAX_SAFE_INTEGER - 1000000;
const CLEANUP_TIMEOUT_MS = 5000; // Timeout for cleanup operations

let retryCount = 0;
let client = null;
let rabbitConn = null;
let rabbitChannel = null;
let changeStreams = [];
let isStopping = false;
let isRestarting = false;
let startPromise = null;
let restartTimer = null;
let isHealthy = false;
let lastError = null;
let retryResetTimer = null;
let stableSince = null;

// ⚙️ Config sanity checks (helps avoid "closed unexpectedly" restart loops)
if (Number.isFinite(MONGO_SOCKET_TIMEOUT_MS) && MONGO_SOCKET_TIMEOUT_MS > 0) {
  // Leave some headroom for network jitter / event loop delay.
  const recommendedMin = MONGO_MAX_AWAIT_TIME_MS + 5000;
  if (MONGO_SOCKET_TIMEOUT_MS < recommendedMin) {
    console.warn(
      `⚠️ Config warning: MONGO_SOCKET_TIMEOUT_MS (${MONGO_SOCKET_TIMEOUT_MS}ms) is too low for change streams with ` +
      `MONGO_MAX_AWAIT_TIME_MS (${MONGO_MAX_AWAIT_TIME_MS}ms). Consider setting socket timeout to 0, or >= ${recommendedMin}ms.`
    );
  }
}

// 🚦 Queue state
let processingQueue = [];
let activeProcessing = 0;
let queuePaused = false;
let totalProcessed = 0;
let totalDropped = 0;

// ⏰ Interval timers
let statusLogInterval = null;
let healthCheckInterval = null;

// 🏥 Health Check Server with diagnostics
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    const status = {
      status: isHealthy && !isStopping && !isRestarting ? 'UP' : 'DOWN',
      retryCount,
      uptime: process.uptime(),
      memoryUsage: process.memoryUsage().heapUsed,
      mongoConnected: !!client,
      rabbitConnected: !!rabbitChannel,
      watchingCollections: changeStreams.length,
      queue: {
        size: processingQueue.length,
        maxSize: MAX_QUEUE_SIZE,
        activeProcessing,
        maxConcurrent: MAX_CONCURRENT_PROCESSING,
        paused: queuePaused,
        totalProcessed,
        totalDropped
      },
      lastError: lastError ? lastError.substring(0, 200) : null,
      timestamp: new Date().toISOString()
    };
    const statusCode = status.status === 'UP' ? 200 : 503;
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(status));
  } else {
    res.writeHead(404);
    res.end();
  }
});

healthServer.on('error', (err) => {
  console.error('🏥❌ Health server error:', err.message);
});

healthServer.listen(healthPort, () => {
  console.log(`🏥 Health check server listening on port ${healthPort}`);
});

function getRetryDelay(count = retryCount) {
  const delay = Math.min(Math.pow(2, count) * 1000, MAX_RETRY_DELAY);
  return delay;
}

// 📊 Periodic status logging - know it's alive without hitting /health
function logPeriodicStatus() {
  const memMB = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`📊 Status: processed=${totalProcessed}, dropped=${totalDropped}, queue=${processingQueue.length}, active=${activeProcessing}, memory=${memMB.toFixed(2)}MB, uptime=${Math.floor(process.uptime() / 60)}min`);
  
  // 🚨 Auto-restart if memory usage is too high (prevent OOM kill)
  if (memMB > MAX_MEMORY_MB) {
    console.warn(`⚠️ Memory usage (${memMB.toFixed(0)}MB) exceeds limit (${MAX_MEMORY_MB}MB). Triggering restart...`);
    scheduleRestart();
  }
  
  // 🔄 Reset counters to prevent integer overflow after months of running
  if (totalProcessed > COUNTER_RESET_THRESHOLD || totalDropped > COUNTER_RESET_THRESHOLD) {
    console.log('📊 Resetting counters to prevent overflow');
    totalProcessed = 0;
    totalDropped = 0;
  }
}

// 🏥 Proactive health check - detect dead connections faster than heartbeats
async function proactiveHealthCheck() {
  if (!isHealthy || isStopping || isRestarting) return;
  
  try {
    // Ping MongoDB to verify connection is alive
    if (client) {
      await client.db('admin').command({ ping: 1 });
    } else {
      throw new Error('MongoDB client not available');
    }
    
    // Verify RabbitMQ channel is still open
    if (!rabbitChannel) {
      throw new Error('RabbitMQ channel not available');
    }
  } catch (err) {
    console.error('❌ Proactive health check failed:', err.message);
    lastError = `Health check: ${err.message}`;
    scheduleRestart();
  }
}

// 📸 Enable pre/post images on collections for before/after document capture
async function ensurePrePostImagesEnabled(db, collectionName) {
  try {
    // Get collection info to check current settings
    const collInfos = await db.listCollections({ name: collectionName }).toArray();
    
    if (collInfos.length === 0) {
      console.warn(`⚠️ Collection [${collectionName}] does not exist yet. Pre/post images will be enabled when created.`);
      return false;
    }
    
    const collInfo = collInfos[0];
    const prePostEnabled = collInfo.options?.changeStreamPreAndPostImages?.enabled === true;
    
    if (prePostEnabled) {
      console.log(`✅ Pre/post images already enabled for [${collectionName}]`);
      return true;
    }
    
    // Enable pre/post images
    console.log(`🔧 Enabling pre/post images for [${collectionName}]...`);
    await db.command({
      collMod: collectionName,
      changeStreamPreAndPostImages: { enabled: true }
    });
    console.log(`✅ Pre/post images enabled for [${collectionName}]`);
    return true;
  } catch (err) {
    // Handle common errors gracefully
    if (err.code === 72 || err.message.includes('Invalid collection option')) {
      console.warn(`⚠️ MongoDB version may not support pre/post images (requires 6.0+): ${err.message}`);
    } else if (err.code === 26 || err.message.includes('ns not found')) {
      console.warn(`⚠️ Collection [${collectionName}] not found. Will be created on first insert.`);
    } else {
      console.warn(`⚠️ Could not enable pre/post images for [${collectionName}]: ${err.message}`);
    }
    return false;
  }
}

async function connectRabbitMQ() {
  while (!isStopping) {
    try {
      // 🐰 Connect with heartbeat for connection health monitoring
      const connectionUrl = rabbitmqUrl.includes('?') 
        ? `${rabbitmqUrl}&heartbeat=${RABBITMQ_HEARTBEAT}`
        : `${rabbitmqUrl}?heartbeat=${RABBITMQ_HEARTBEAT}`;
      
      const connection = await amqp.connect(connectionUrl);
      
      connection.on('error', (err) => {
        if (!isStopping && !isRestarting) {
          lastError = `RabbitMQ Connection: ${err.message}`;
          console.error('❌ RabbitMQ Connection Error:', err.message);
          scheduleRestart();
        }
      });
      
      connection.on('close', () => {
        if (!isStopping && !isRestarting) {
          lastError = 'RabbitMQ connection closed';
          console.warn('⚠️ RabbitMQ Connection Closed.');
          scheduleRestart();
        }
      });
      
      // 🔧 Create confirm channel with error handlers
      const channel = await connection.createConfirmChannel();
      
      channel.on('error', (err) => {
        if (!isStopping && !isRestarting) {
          lastError = `RabbitMQ Channel: ${err.message}`;
          console.error('❌ RabbitMQ Channel Error:', err.message);
          scheduleRestart();
        }
      });
      
      channel.on('close', () => {
        if (!isStopping && !isRestarting) {
          lastError = 'RabbitMQ channel closed';
          console.warn('⚠️ RabbitMQ Channel Closed.');
          scheduleRestart();
        }
      });
      
      await channel.assertQueue(queueName, { durable: true });
      
      // 🚦 Prefetch limit to control memory usage
      await channel.prefetch(MAX_CONCURRENT_PROCESSING);
      
      console.log('✅ Connected to RabbitMQ (Confirm Channel, Heartbeat: ' + RABBITMQ_HEARTBEAT + 's, Prefetch: ' + MAX_CONCURRENT_PROCESSING + ')');
      rabbitConn = connection;
      rabbitChannel = channel;
      return channel;
    } catch (error) {
      if (isStopping) break;
      lastError = `RabbitMQ Connect: ${error.message}`;
      const delay = getRetryDelay();
      console.error(`❌ Failed to connect to RabbitMQ. Retrying in ${delay / 1000}s...`);
      retryCount++;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

async function connectMongoDB() {
  while (!isStopping) {
    try {
      // 🍃 MongoDB connection with robust settings for long-running operations
      const mongoClient = new MongoClient(mongoUri, {
        serverSelectionTimeoutMS: MONGO_SERVER_SELECTION_TIMEOUT_MS,
        heartbeatFrequencyMS: MONGO_HEARTBEAT_MS,
        socketTimeoutMS: MONGO_SOCKET_TIMEOUT_MS,
        maxPoolSize: 10,
        minPoolSize: 2,
        retryWrites: true,
        retryReads: true,
      });
      
      await mongoClient.connect();
      
      // 🔔 Listen for topology events to detect disconnections
      mongoClient.on('serverDescriptionChanged', (event) => {
        const { newDescription } = event;
        if (newDescription.type === 'Unknown' && !isStopping) {
          console.warn('⚠️ MongoDB server became unavailable');
          lastError = 'MongoDB server unavailable';
        }
      });
      
      mongoClient.on('topologyDescriptionChanged', (event) => {
        const { newDescription } = event;
        const servers = Array.from(newDescription.servers.values());
        const allDown = servers.every(s => s.type === 'Unknown');
        if (allDown && servers.length > 0 && !isStopping && !isRestarting) {
          console.error('❌ All MongoDB servers are down');
          lastError = 'All MongoDB servers down';
          scheduleRestart();
        }
      });
      
      mongoClient.on('error', (err) => {
        if (!isStopping && !isRestarting) {
          lastError = `MongoDB: ${err.message}`;
          console.error('❌ MongoDB Client Error:', err.message);
          scheduleRestart();
        }
      });
      
      mongoClient.on('close', () => {
        if (!isStopping && !isRestarting) {
          lastError = 'MongoDB connection closed';
          console.warn('⚠️ MongoDB Connection Closed.');
          scheduleRestart();
        }
      });
      
      console.log('✅ Connected to MongoDB (Heartbeat: ' + (MONGO_HEARTBEAT_MS / 1000) + 's)');
      client = mongoClient;
      return mongoClient;
    } catch (error) {
      if (isStopping) break;
      lastError = `MongoDB Connect: ${error.message}`;
      const delay = getRetryDelay();
      console.error(`❌ Failed to connect to MongoDB. Retrying in ${delay / 1000}s...`);
      retryCount++;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

// ⏱️ Helper to wrap operations with timeout (prevent hanging)
async function withTimeout(fn, name, timeoutMs = CLEANUP_TIMEOUT_MS) {
  try {
    await Promise.race([
      fn(),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error(`Timeout after ${timeoutMs}ms`)), timeoutMs)
      )
    ]);
  } catch (err) {
    console.warn(`⚠️ ${name}: ${err.message}`);
  }
}

// 🧹 Cleanup resources without fully stopping (for restart scenarios)
async function cleanup() {
  console.log('🧹 Cleaning up resources...');
  isHealthy = false;
  stableSince = null;

  // ⏰ Clear interval timers first
  if (statusLogInterval) {
    clearInterval(statusLogInterval);
    statusLogInterval = null;
  }
  if (healthCheckInterval) {
    clearInterval(healthCheckInterval);
    healthCheckInterval = null;
  }
  if (retryResetTimer) {
    clearTimeout(retryResetTimer);
    retryResetTimer = null;
  }

  // Close change streams first (with timeout)
  for (const stream of changeStreams) {
    await withTimeout(() => stream.close(), 'Close change stream');
  }
  changeStreams = [];

  // Close RabbitMQ channel and connection (with timeout)
  if (rabbitChannel) {
    await withTimeout(() => rabbitChannel.close(), 'Close RabbitMQ channel');
    rabbitChannel = null;
  }

  if (rabbitConn) {
    await withTimeout(() => rabbitConn.close(), 'Close RabbitMQ connection');
    rabbitConn = null;
  }

  // Close MongoDB client (with timeout)
  if (client) {
    await withTimeout(() => client.close(true), 'Close MongoDB client');
    client = null;
  }
  
  console.log('🧹 Cleanup complete.');
}

async function stop() {
  if (isStopping) return;
  isStopping = true;
  isHealthy = false;
  console.log('🛑 Stopping listener gracefully...');

  if (restartTimer) {
    clearTimeout(restartTimer);
    restartTimer = null;
  }

  await cleanup();
  
  console.log('👋 Cleanly exited.');
}

// 📦 Build a clean message payload with before/after documents and changed fields
function buildMessagePayload(name, change) {
  const payload = {
    collection: name,
    operationType: change.operationType,
    documentKey: change.documentKey,
    // 📄 Document states
    documentBefore: change.fullDocumentBeforeChange || null, // Before the change (MongoDB 6.0+)
    documentAfter: change.fullDocument || null, // After the change
    // 📝 Specific field changes (for updates)
    changedFields: null,
    removedFields: null,
    truncatedArrays: null,
    // ⏰ Timestamps
    clusterTime: change.clusterTime,
    wallTime: change.wallTime || new Date().toISOString(),
  };
  
  // 📝 For update operations, include the specific changed/removed fields
  if (change.updateDescription) {
    payload.changedFields = change.updateDescription.updatedFields || null;
    payload.removedFields = change.updateDescription.removedFields || [];
    payload.truncatedArrays = change.updateDescription.truncatedArrays || [];
  }
  
  // 🗑️ For delete operations, documentAfter will be null
  // but documentBefore should have the deleted document (if enabled)
  if (change.operationType === 'delete') {
    payload.documentAfter = null;
  }
  
  // ➕ For insert operations, documentBefore will be null
  if (change.operationType === 'insert') {
    payload.documentBefore = null;
  }
  
  return payload;
}

// 🚦 Queue processor - processes changes with concurrency control
async function processQueueItem(item, channel, syncCollection) {
  const { name, change, resumeToken } = item;
  
  try {
    const payload = buildMessagePayload(name, change);
    const message = JSON.stringify(payload);

    await new Promise((resolve, reject) => {
      channel.sendToQueue(queueName, Buffer.from(message), { persistent: true }, (err, ok) => {
        if (err) return reject(err);
        resolve(ok);
      });
    });

    await syncCollection.updateOne(
      { _id: name },
      { $set: { resumeToken, updatedAt: new Date() } },
      { upsert: true }
    );
    
    totalProcessed++;
  } catch (error) {
    throw error; // Re-throw to be handled by caller
  }
}

async function drainQueue(channel, syncCollection) {
  while (processingQueue.length > 0 && activeProcessing < MAX_CONCURRENT_PROCESSING && !isStopping && !isRestarting) {
    const item = processingQueue.shift();
    if (!item) break;
    
    activeProcessing++;
    
    // Process without awaiting to allow concurrent processing
    processQueueItem(item, channel, syncCollection)
      .catch((error) => {
        lastError = `Process change [${item.name}]: ${error.message}`;
        console.error(`❌ Failed to process change for [${item.name}]:`, error.message);
        // 🚨 CRITICAL: If we can't send or sync, we must stop and retry from the last token
        scheduleRestart();
      })
      .finally(() => {
        activeProcessing--;
        // Continue draining if there are more items
        if (processingQueue.length > 0 && !isStopping && !isRestarting) {
          setImmediate(() => drainQueue(channel, syncCollection));
        }
      });
  }
}

function enqueueChange(name, change, resumeToken, channel, syncCollection) {
  if (isStopping || isRestarting) return;
  
  // 🚦 Check queue size - apply backpressure
  if (processingQueue.length >= MAX_QUEUE_SIZE) {
    if (!queuePaused) {
      queuePaused = true;
      console.warn(`⚠️ Queue full (${MAX_QUEUE_SIZE}). Applying backpressure...`);
    }
    totalDropped++;
    // Drop oldest item to make room (or you could drop newest - depends on your needs)
    processingQueue.shift();
  }
  
  // Add to queue
  processingQueue.push({ name, change, resumeToken });
  
  // Resume if was paused and now has room
  if (queuePaused && processingQueue.length < MAX_QUEUE_SIZE * 0.8) {
    queuePaused = false;
    console.log('✅ Queue backpressure released.');
  }
  
  // Trigger processing
  drainQueue(channel, syncCollection);
}

async function scheduleRestart() {
  if (isStopping || isRestarting) return;
  
  isRestarting = true;
  isHealthy = false;
  stableSince = null;
  
  // 🧹 Clean up existing connections before scheduling restart
  await cleanup();
  
  // 🧹 Clear the processing queue on restart
  const queueSize = processingQueue.length;
  if (queueSize > 0) {
    console.log(`🧹 Clearing ${queueSize} items from processing queue...`);
    processingQueue = [];
  }
  activeProcessing = 0;
  queuePaused = false;
  
  const delay = getRetryDelay(retryCount);
  console.log(`🔄 Restart scheduled in ${delay / 1000}s (Retry #${retryCount + 1})`);
  retryCount++;
  
  if (restartTimer) {
    clearTimeout(restartTimer);
  }
  
  restartTimer = setTimeout(async () => {
    restartTimer = null;
    isRestarting = false;
    try {
      await start();
    } catch (err) {
      lastError = `Restart: ${err.message}`;
      console.error('💥 Error during restart:', err.message);
      scheduleRestart();
    }
  }, delay);
}

async function start() {
  if (startPromise) return startPromise;

  startPromise = (async () => {
    try {
      // Reset flags for fresh start
      isStopping = false;
      isRestarting = false;
      lastError = null;

      console.log('🚀 Starting MongoDB Change Stream Listener...');

      const channel = await connectRabbitMQ();
      if (!channel) {
        throw new Error('Failed to establish RabbitMQ channel');
      }
      
      const mongoClient = await connectMongoDB();
      if (!mongoClient) {
        throw new Error('Failed to establish MongoDB connection');
      }
      
      if (isStopping) {
        return;
      }

      // Not healthy until all streams are initialized successfully
      isHealthy = false;

      const db = mongoClient.db(dbName);
      const syncCollection = db.collection(syncCollectionName);

      // 📸 Auto-enable pre/post images on all watched collections
      console.log('📸 Checking pre/post image settings for collections...');
      for (const name of collectionNames) {
        await ensurePrePostImagesEnabled(db, name);
      }

      for (const name of collectionNames) {
        const collection = db.collection(name);
        const syncState = await syncCollection.findOne({ _id: name });
        const resumeAfter = syncState ? syncState.resumeToken : null;
        
        // 🔧 Change stream with options for better reliability
        const changeStreamOptions = {
          ...(resumeAfter ? { resumeAfter } : {}),
          maxAwaitTimeMS: MONGO_MAX_AWAIT_TIME_MS, // Max time to wait for new changes
          fullDocument: 'updateLookup', // Get full document after change
          fullDocumentBeforeChange: 'whenAvailable', // Get full document before change (MongoDB 6.0+)
        };
        
        const changeStream = collection.watch([], changeStreamOptions);
        changeStreams.push(changeStream);
        
        console.log(`👀 Watching ${dbName}.${name} (Resuming: ${resumeAfter ? 'Yes' : 'No'})...`);

        changeStream.on('change', (change) => {
          if (isStopping || isRestarting) return;
          const resumeToken = change._id;
          
          // 🚦 Enqueue the change for rate-limited processing
          enqueueChange(name, change, resumeToken, channel, syncCollection);
        });

        changeStream.on('error', async (error) => {
          if (!isStopping && !isRestarting) {
            lastError = `Change Stream [${name}]: ${error.message}`;
            console.error(`❌ Change Stream Error for [${name}]:`, error.message);
            
            // 🚨 Handle stale/expired resume tokens (oplog expires after 24h-7d)
            const isResumeTokenError = 
              error.code === 286 || 
              error.codeName === 'ChangeStreamHistoryLost' ||
              error.message.includes('resume token') || 
              error.message.includes('oplog') ||
              error.message.includes('ChangeStreamHistoryLost');
            
            if (isResumeTokenError) {
              console.warn(`⚠️ Resume token expired for [${name}]. Clearing token to restart from current position.`);
              try {
                await syncCollection.deleteOne({ _id: name });
              } catch (deleteErr) {
                console.warn(`⚠️ Failed to clear resume token: ${deleteErr.message}`);
              }
            }
            
            scheduleRestart();
          }
        });
        
        // 📢 Listen for stream close events
        changeStream.on('close', () => {
          if (!isStopping && !isRestarting) {
            lastError = `Change Stream [${name}] closed unexpectedly`;
            console.warn(`⚠️ Change Stream for [${name}] closed unexpectedly`);
            scheduleRestart();
          }
        });
      }
      
      // ⏰ Start periodic monitoring intervals
      statusLogInterval = setInterval(logPeriodicStatus, STATUS_LOG_INTERVAL_MS);
      healthCheckInterval = setInterval(proactiveHealthCheck, HEALTH_CHECK_INTERVAL_MS);
      
      console.log('✅ All change streams initialized successfully!');
      console.log(`📊 Status logging every ${STATUS_LOG_INTERVAL_MS / 1000}s, health check every ${HEALTH_CHECK_INTERVAL_MS / 1000}s`);

      // ✅ Mark healthy and only reset retry counter after a stable period (prevents 1s restart storms)
      isHealthy = true;
      stableSince = Date.now();
      const stableMark = stableSince;
      if (retryResetTimer) clearTimeout(retryResetTimer);
      retryResetTimer = setTimeout(() => {
        if (
          !isStopping &&
          !isRestarting &&
          isHealthy &&
          stableSince === stableMark &&
          retryCount !== 0
        ) {
          console.log(`✅ Stable for ${Math.round(RETRY_RESET_AFTER_MS / 1000)}s. Resetting retry counter.`);
          retryCount = 0;
        }
      }, RETRY_RESET_AFTER_MS);
    } catch (err) {
      lastError = `Start: ${err.message}`;
      console.error('💥 Start sequence failed:', err.message);
      scheduleRestart();
    } finally {
      startPromise = null;
    }
  })();

  return startPromise;
}

// 🛡️ Signal Listeners for Graceful Shutdown
process.on('SIGTERM', async () => {
  console.log('📥 Received SIGTERM');
  await stop();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('📥 Received SIGINT');
  await stop();
  process.exit(0);
});

// 🚨 Critical: Handle uncaught errors to prevent crashes
process.on('uncaughtException', (error) => {
  console.error('🚨 Uncaught Exception:', error.message);
  console.error(error.stack);
  lastError = `Uncaught: ${error.message}`;
  // Don't exit - let the restart logic handle it
  scheduleRestart();
});

process.on('unhandledRejection', (reason, promise) => {
  const message = reason instanceof Error ? reason.message : String(reason);
  console.error('🚨 Unhandled Rejection:', message);
  if (reason instanceof Error) {
    console.error(reason.stack);
  }
  lastError = `Unhandled: ${message}`;
  // Don't exit - let the restart logic handle it
  scheduleRestart();
});

// 🎬 Start the listener
console.log('═══════════════════════════════════════════════════════════');
console.log('🚀 MongoDB Change Stream Listener - Production Ready');
console.log('═══════════════════════════════════════════════════════════');
console.log(`📦 Database: ${dbName}`);
console.log(`📋 Collections: ${collectionNames.join(', ')}`);
console.log(`🐰 RabbitMQ Queue: ${queueName}`);
console.log(`🚦 Max Concurrent: ${MAX_CONCURRENT_PROCESSING}`);
console.log(`🚦 Max Queue Size: ${MAX_QUEUE_SIZE}`);
console.log(`💾 Max Memory: ${MAX_MEMORY_MB}MB`);
console.log(`🏥 Health Port: ${healthPort}`);
console.log('═══════════════════════════════════════════════════════════');

start();

