# Quantiva v3.0 — RSI Worker

RabbitMQ consumer that handles RSI computation jobs dispatched by the orchestrator. Reads candle data from Redis by key, computes RSI, and replies to `response_queue` with the correlation ID for orchestrator matching.

---

## Role in Pipeline

```
Orchestrator → rsi_queue
                    ↓
              w_rsi consumes message
                    ↓ reads candles from Redis (key: ticker:startDate:endDate)
                    ↓ computes RSI (period=2, simple average method)
              response_queue ← { type: "rsi", result: float }
                    ↓ correlationId matched by orchestrator
```

---

## RSI Implementation

Period defaults to 2. Uses simple average gain/loss method (not Wilder's smoothing):

```
gains  = sum of all positive close deltas
losses = sum of all negative close deltas (absolute)
avgGain = gains / period
avgLoss = losses / period
RS  = avgGain / avgLoss
RSI = 100 - (100 / (1 + RS))
```

> Note: iterates all candles for gain/loss accumulation but divides by `period` only — diverges from standard Wilder RSI which uses a rolling average. Intentional simplification.

---

## Message Contract

**Consumed from `rsi_queue`:**
```json
{ "type": "rsi", "key": "AAPL:2025-01-01:2025-06-01" }
```

**Published to `response_queue`:**
```json
{ "type": "rsi", "result": 61.34 }
```

Correlation ID passed through unchanged — orchestrator uses it to match all 3 worker replies.

---

## Stack

| Layer     | Technology            |
|-----------|-----------------------|
| Runtime   | Node.js 20+ (ESM)     |
| Messaging | RabbitMQ (amqplib)    |
| Cache     | Redis (read-only)     |
| HTTP      | Express (health only) |

---

## Environment Variables

| Variable      | Description                |
|---------------|----------------------------|
| `REDIS_KEY`   | Redis connection string    |
| `RABITMQ_KEY` | RabbitMQ connection string |
| `PORT`        | HTTP port (default: 3002)  |

---

## Running

```bash
npm install
node index.js
```

Queue is `durable: true` — messages persist if worker is down and are processed on reconnect.

---

## Endpoints

| Method | Path      | Returns                            |
|--------|-----------|------------------------------------|
| GET    | `/`       | `{ message: "Server is running" }` |
| GET    | `/health` | `{ status: "ok", uptime: N }`      |
