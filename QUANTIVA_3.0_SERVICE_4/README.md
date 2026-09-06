# Quantiva v3.0 — SMA Worker

RabbitMQ consumer that handles SMA computation jobs dispatched by the orchestrator. Reads candle data from Redis by key, computes SMA over the last N closes, and replies to `response_queue` with the correlation ID for orchestrator matching.

---

## Role in Pipeline

```
Orchestrator → sma_queue
                    ↓
              w_sma consumes message
                    ↓ reads candles from Redis (key: ticker:startDate:endDate)
                    ↓ computes SMA (period=3, last N closes)
              response_queue ← { type: "sma", result: float }
                    ↓ correlationId matched by orchestrator
```

---

## SMA Implementation

Period defaults to 3. Slices the last `period` candles and averages their close prices:

```
SMA = sum(closes[-period:]) / period
```

Single-pass, O(period). No historical warmup required.

---

## Message Contract

**Consumed from `sma_queue`:**
```json
{ "type": "sma", "key": "AAPL:2025-01-01:2025-06-01" }
```

**Published to `response_queue`:**
```json
{ "type": "sma", "result": 211.93 }
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
| `PORT`        | HTTP port (default: 3003)  |

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
