import express from "express";
import cors from "cors";
import YahooFinance from "yahoo-finance2";
const yahooFinance = new YahooFinance();
import { createClient } from 'redis';
import dotenv from "dotenv";
import amqplib from "amqplib";

dotenv.config();

// ── Redis ─────────────────────────────────────────────────────────────────────
const client = createClient({ url: process.env.REDIS_KEY });
await client.connect();
client.on('error', err => console.log('Redis Client Error', err));

// ── RabbitMQ ──────────────────────────────────────────────────────────────────
const rabbitmqUrl = process.env.RABITMQ_KEY;
const responseQueue = "response_queue";

let conn = await amqplib.connect(rabbitmqUrl);
let channel = await conn.createChannel();

await channel.assertQueue(responseQueue, { durable: false });
await channel.assertQueue("main_queue", { durable: true });
await channel.assertExchange("amq.direct", "direct", { durable: true });

const pending = new Map();

channel.consume(responseQueue, (msg) => {
  const id = msg.properties.correlationId;
  const req = pending.get(id);
  if (!req) return channel.ack(msg);
  req.collected.push(JSON.parse(msg.content.toString()));
  channel.ack(msg);
  if (req.collected.length === 3) {
    req.resolve(req.collected);
    pending.delete(id);
  }
}, { noAck: false });

// ── Sequential baseline computations (mirrors worker logic) ──────────────────
function computeEMA(closes) {
  const k = 2 / (closes.length + 1);
  return closes.reduce((ema, price, i) => i === 0 ? price : price * k + ema * (1 - k), closes[0]);
}

function computeSMA(closes) {
  return closes.reduce((a, b) => a + b, 0) / closes.length;
}

function computeRSI(closes, period = 14) {
  if (closes.length < period + 1) return null;
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    diff >= 0 ? (gains += diff) : (losses -= diff);
  }
  let avgGain = gains / period;
  let avgLoss = losses / period;
  for (let i = period + 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    avgGain = (avgGain * (period - 1) + Math.max(diff, 0)) / period;
    avgLoss = (avgLoss * (period - 1) + Math.max(-diff, 0)) / period;
  }
  return avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
}

// ── App ───────────────────────────────────────────────────────────────────────
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(cors());

app.get("/", (req, res) => res.json({ message: "Server is running" }));
app.get("/health", (req, res) => res.json({ status: "ok", uptime: process.uptime() }));

// ── Main analysis endpoint ────────────────────────────────────────────────────
app.post("/initiate-company-analysis", async (req, res) => {
  const { ticker, startDate, endDate } = req.body;

  try {
    const senderQueue = ["ema_queue", "rsi_queue", "sma_queue"];
    const key = `${ticker}:${startDate}:${endDate}`;
    const reqStart = Date.now();

    // Cache check
    const cachedData = await client.get(key);
    const cacheCheckMs = Date.now() - reqStart;

    if (cachedData) {
      console.log(`[METRICS] Cache HIT — ${cacheCheckMs}ms`);
      const correlationId = crypto.randomUUID();

      const fanOutStart = Date.now();
      for (const queue of senderQueue) {
        channel.sendToQueue(queue,
          Buffer.from(JSON.stringify({ type: queue.split("_")[0], key })),
          { persistent: true, correlationId, replyTo: responseQueue }
        );
      }

      const results = await Promise.race([
        new Promise((resolve) => pending.set(correlationId, { collected: [], resolve })),
        new Promise((_, reject) => setTimeout(() => reject(new Error("Worker timeout after 15s")), 15000)),
      ]);

      const workerMs = Date.now() - fanOutStart;
      const totalMs = Date.now() - reqStart;

      console.log(`[METRICS] Workers (cache hit): ${workerMs}ms | Total: ${totalMs}ms`);

      return res.json({
        results,
        source: "cache",
        cache: true,
        data: JSON.parse(cachedData),
        metrics: {
          cache_hit: true,
          cache_check_ms: cacheCheckMs,
          worker_roundtrip_ms: workerMs,
          total_ms: totalMs,
        },
      });
    }

    // Fetch from Yahoo Finance
    const fetchStart = Date.now();
    const data = await yahooFinance.historical(ticker, { period1: startDate, period2: endDate });
    const fetchMs = Date.now() - fetchStart;

    if (!data || data.length === 0) {
      return res.status(404).json({ error: `No data found for ticker: ${ticker}` });
    }

    const d = data.map((entry) => ({
      date: entry.date,
      open: entry.open,
      high: entry.high,
      low: entry.low,
      close: entry.close,
      volume: entry.volume,
    }));

    // Cache for 1 hour
    await client.setEx(key, 3600, JSON.stringify(d));

    // Fan out to workers
    const correlationId = crypto.randomUUID();
    const fanOutStart = Date.now();

    for (const queue of senderQueue) {
      channel.sendToQueue(queue,
        Buffer.from(JSON.stringify({ type: queue.split("_")[0], key })),
        { persistent: true, correlationId, replyTo: responseQueue }
      );
    }

    const results = await Promise.race([
      new Promise((resolve) => pending.set(correlationId, { collected: [], resolve })),
      new Promise((_, reject) => setTimeout(() => reject(new Error("Worker timeout after 15s")), 15000)),
    ]);

    const workerMs = Date.now() - fanOutStart;
    const totalMs = Date.now() - reqStart;

    console.log(`[METRICS] Yahoo fetch: ${fetchMs}ms | Workers: ${workerMs}ms | Total: ${totalMs}ms`);

    return res.json({
      results,
      source: "fetched",
      cache: false,
      data: d,
      metrics: {
        cache_hit: false,
        yahoo_fetch_ms: fetchMs,
        worker_roundtrip_ms: workerMs,
        total_ms: totalMs,
      },
    });

  } catch (error) {
    console.error("Error:", error.message);
    res.status(500).json({ error: error.message || "Failed to fetch data" });
  }
});

// ── Benchmark endpoint ────────────────────────────────────────────────────────
// Runs the same ticker through BOTH paths and returns a side-by-side comparison.
// Sequential path: computes EMA, SMA, RSI one-by-one in this process (no queue).
// Distributed path: fans out to workers via RabbitMQ (uses cache if warm).
// Use this to generate real numbers for your resume.
//
// POST /benchmark  { "ticker": "AAPL", "startDate": "2025-01-01", "endDate": "2025-06-01" }

app.post("/benchmark", async (req, res) => {
  const { ticker, startDate, endDate } = req.body;

  if (!ticker || !startDate || !endDate) {
    return res.status(400).json({ error: "ticker, startDate, endDate required" });
  }

  try {
    const key = `${ticker}:${startDate}:${endDate}`;

    // ── Step 1: Get candle data (shared by both paths) ────────────────────────
    const fetchStart = Date.now();
    let candles;
    let dataSource;

    const cached = await client.get(key);
    if (cached) {
      candles = JSON.parse(cached);
      dataSource = "cache";
    } else {
      const raw = await yahooFinance.historical(ticker, { period1: startDate, period2: endDate });
      candles = raw.map(e => ({
        date: e.date, open: e.open, high: e.high,
        low: e.low, close: e.close, volume: e.volume,
      }));
      await client.setEx(key, 3600, JSON.stringify(candles));
      dataSource = "yahoo_finance";
    }

    const fetchMs = Date.now() - fetchStart;
    const closes = candles.map(c => c.close);

    // ── Step 2: Sequential path — no queue, no parallelism ───────────────────
    const seqStart = Date.now();

    const t0 = Date.now();
    const seqEMA = computeEMA(closes);
    const emaMs = Date.now() - t0;

    const t1 = Date.now();
    const seqSMA = computeSMA(closes);
    const smaMs = Date.now() - t1;

    const t2 = Date.now();
    const seqRSI = computeRSI(closes);
    const rsiMs = Date.now() - t2;

    const sequentialMs = Date.now() - seqStart;

    // ── Step 3: Distributed path — fan out to RabbitMQ workers ───────────────
    const correlationId = crypto.randomUUID();
    const senderQueue = ["ema_queue", "rsi_queue", "sma_queue"];

    const distStart = Date.now();

    for (const queue of senderQueue) {
      channel.sendToQueue(queue,
        Buffer.from(JSON.stringify({ type: queue.split("_")[0], key })),
        { persistent: true, correlationId, replyTo: responseQueue }
      );
    }

    const distResults = await Promise.race([
      new Promise((resolve) => pending.set(correlationId, { collected: [], resolve })),
      new Promise((_, reject) => setTimeout(() => reject(new Error("Worker timeout")), 15000)),
    ]);

    const distributedMs = Date.now() - distStart;

    // ── Step 4: Build comparison ──────────────────────────────────────────────
    const speedup = (sequentialMs / distributedMs).toFixed(2);
    const savedMs = sequentialMs - distributedMs;

    const comparison = {
      ticker,
      date_range: { from: startDate, to: endDate },
      candles: candles.length,
      data_source: dataSource,
      data_fetch_ms: fetchMs,

      sequential: {
        description: "EMA → SMA → RSI computed one-by-one in a single process, no message queue",
        total_ms: sequentialMs,
        breakdown: {
          ema_ms: emaMs,
          sma_ms: smaMs,
          rsi_ms: rsiMs,
        },
        results: [
          { type: "ema", result: seqEMA },
          { type: "sma", result: seqSMA },
          { type: "rsi", result: seqRSI },
        ],
      },

      distributed: {
        description: "EMA, SMA, RSI fanned out simultaneously to 3 independent RabbitMQ workers",
        total_ms: distributedMs,
        results: distResults,
      },

      verdict: {
        speedup_factor: `${speedup}x`,
        time_saved_ms: savedMs,
        faster: distributedMs < sequentialMs ? "distributed" : "sequential",
        note: savedMs > 0
          ? `Distributed workers completed ${savedMs}ms faster than sequential execution`
          : `Sequential was faster — workers likely had queue/network overhead on this run. Run again on warm services for accurate comparison.`,
      },
    };

    console.log(`[BENCHMARK] Sequential: ${sequentialMs}ms | Distributed: ${distributedMs}ms | Speedup: ${speedup}x`);

    return res.json(comparison);

  } catch (error) {
    console.error("[BENCHMARK] Error:", error.message);
    res.status(500).json({ error: error.message });
  }
});

// ── Dev helper endpoint ───────────────────────────────────────────────────────
app.get("/send-message-via-queue/:message", async (req, res) => {
  try {
    await channel.sendToQueue("ema_queue", Buffer.from(req.params.message), { persistent: true });
    res.json({ status: "sent", message: req.params.message });
  } catch (e) {
    res.status(500).json({ error: "Failed to send" });
  }
});

app.use((req, res) => res.status(404).json({ error: "Not found" }));
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: "Internal server error" });
});

app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));