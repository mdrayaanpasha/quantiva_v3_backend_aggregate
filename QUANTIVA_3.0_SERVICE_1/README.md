# Quantiva v3.0 — Orchestrator Server

Central API server for the Quantiva v3.0 pipeline. Fetches OHLCV data from Yahoo Finance, caches it in Redis, then fans out EMA/SMA/RSI computation jobs to three independent RabbitMQ workers concurrently. Correlates async replies via UUID correlation IDs.

**Architecture pattern:** RabbitMQ RPC fan-out with cache-aside

---

## Architecture

```
POST /initiate-company-analysis
        ↓
Redis cache check (key: ticker:startDate:endDate)
        ├── HIT  → skip Yahoo Finance (~sub-10ms)
        └── MISS → Yahoo Finance fetch (~750ms) → cache for 1hr
        ↓
Fan-out → ema_queue
        → rsi_queue       (concurrent, via amq.direct exchange)
        → sma_queue
        ↓
Workers compute independently, reply to response_queue
        ↓ correlationId matches all 3 replies
Orchestrator resolves → returns unified JSON
```

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Health ping |
| GET | `/health` | Uptime + status |
| POST | `/initiate-company-analysis` | Main analysis — fetch + cache + fan-out |
| POST | `/benchmark` | Side-by-side sequential vs distributed comparison |
| GET | `/send-message-via-queue/:message` | Dev helper — raw queue send to `ema_queue` |

### POST `/initiate-company-analysis`

```json
{ "ticker": "AAPL", "startDate": "2025-01-01", "endDate": "2025-06-01" }
```

Response includes `results` (worker output), `data` (OHLCV), `source` (cache/fetched), and `metrics`:

```json
{
  "metrics": {
    "cache_hit": false,
    "yahoo_fetch_ms": 743,
    "worker_roundtrip_ms": 38,
    "total_ms": 791
  }
}
```

### POST `/benchmark`

Runs the same ticker through both paths and returns a side-by-side comparison:

```json
{
  "sequential": { "total_ms": 12, "breakdown": { "ema_ms": 4, "sma_ms": 3, "rsi_ms": 5 } },
  "distributed": { "total_ms": 31 },
  "verdict": { "speedup_factor": "0.39x", "faster": "sequential" }
}
```

> Sequential wins on small datasets due to queue overhead. Distributed wins at scale or when workers are pre-warmed. Use `/benchmark` to generate real numbers.

---

## RabbitMQ Fan-out Design

- Each job is tagged with a `crypto.randomUUID()` correlation ID
- Orchestrator registers a pending resolver in a `Map` keyed by correlation ID
- Workers reply to `response_queue` with the same correlation ID
- Resolver fires when all 3 replies are collected (`collected.length === 3`)
- 15s timeout via `Promise.race` — prevents hung requests if a worker dies

---

## Redis Cache-Aside

- Key format: `ticker:startDate:endDate`
- TTL: 3600s (1 hour)
- Cache hit skips Yahoo Finance entirely — ~750ms → sub-10ms on repeat requests
- Workers read candle data from Redis directly (passed by key, not value)

---

## Stack

| Layer      | Technology              |
|------------|-------------------------|
| Runtime    | Node.js 20+ (ESM)       |
| HTTP       | Express + CORS          |
| Messaging  | RabbitMQ (amqplib)      |
| Cache      | Redis                   |
| Market data | yahoo-finance2         |

---

## Environment Variables

| Variable      | Description                  |
|---------------|------------------------------|
| `REDIS_KEY`   | Redis connection string      |
| `RABITMQ_KEY` | RabbitMQ connection string   |
| `PORT`        | Server port (default: 3000)  |

---

## Running

```bash
npm install
node index.js
```

Workers (`ema_queue`, `rsi_queue`, `sma_queue`) must be running independently before requests are sent — orchestrator will timeout after 15s if no worker replies.
