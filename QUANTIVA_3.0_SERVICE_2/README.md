# Quantiva v3.0 — EMA Worker (`w_ema`)

RabbitMQ consumer that handles EMA computation jobs dispatched by the orchestrator. Reads candle data from Redis by key, computes EMA, and replies to `response_queue` with the correlation ID for orchestrator matching.

---

## Role in Pipeline

```
Orchestrator → ema_queue
                    ↓
              w_ema consumes message
                    ↓ reads candles from Redis (key: ticker:startDate:endDate)
                    ↓ computes EMA (period=3, full-series smoothing)
              response_queue ← { type: "ema", result: float }
                    ↓ correlationId matched by orchestrator
```

---

## EMA Implementation

Period defaults to 3. Uses standard exponential smoothing:

```
k = 2 / (period + 1)
EMA₀ = closes[0]
EMAᵢ = closes[i] * k + EMAᵢ₋₁ * (1 - k)
```

Seeded from first close price, iterated across all candles. Single-pass, O(n).

---

## Message Contract

**Consumed from `ema_queue`:**
```json
{ "type": "ema", "key": "AAPL:2025-01-01:2025-06-01" }
```

**Published to `response_queue`:**
```json
{ "type": "ema", "result": 213.47 }
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
| `PORT`        | HTTP port (default: 3001)  |

---

## Running

```bash
npm install
node index.js
```

Worker must be running before the orchestrator dispatches jobs. If `ema_queue` has no consumer, messages queue up and are processed on reconnect (queue is `durable: true`).

---

## Endpoints

| Method | Path      | Returns              |
|--------|-----------|----------------------|
| GET    | `/`       | `{ message: "Server is running" }` |
| GET    | `/health` | `{ status: "ok", uptime: N }` |
````
