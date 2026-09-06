import express from "express";
import cors from "cors";
import amqplib from "amqplib";
import { createClient } from 'redis';
import dotenv from "dotenv";

dotenv.config();

const client = createClient({
  url: process.env.REDIS_KEY
});
client.on('error', err => console.log('Redis Client Error', err));
await client.connect();

const rabbitmqUrl = process.env.RABITMQ_KEY;

async function connectRabbitMQ() {
  try {
    const connection = await amqplib.connect(rabbitmqUrl);
    channel = await connection.createChannel();
    console.log("Connected to RabbitMQ");
  } catch (error) {
    console.error("Failed to connect to RabbitMQ:", error);
  }
}

let Queue = "ema_queue";

let conn = await amqplib.connect(rabbitmqUrl);
const channel = await conn.createChannel();

await channel.assertQueue(Queue, { durable: true });
await channel.assertExchange("amq.direct", "direct", { durable: true });

await channel.consume(Queue, async (msg) => {

  if (msg !== null) {
    const mess = JSON.parse(msg.content.toString());
    console.log("Received message:", mess);

    if (mess.type === "ema") {
      console.log("here we are...")
      const key = mess.key;

   const analysisData = await client.get(key);
// ADD HERE
if (!analysisData) {
  console.error("No data in Redis for key:", key);
  channel.ack(msg);
  return;
}
const candles = JSON.parse(analysisData);
if (!candles || candles.length === 0) {
  console.error("Empty candles for key:", key);
  channel.ack(msg);
  return;
}

    function calculateEMA(candles, period = 3) {
      const k = 2 / (period + 1);
      let ema = candles[0].close; // start with the first close price

      for (let i = 1; i < candles.length; i++) {
        ema = candles[i].close * k + ema * (1 - k);
      }
      return ema;
    }

    const ema = calculateEMA(candles);
 channel.sendToQueue(
        "response_queue",
        Buffer.from(JSON.stringify({ type: mess.type, result: ema })),
        { correlationId: msg.properties.correlationId }  // ← just this line
        );      
    

    } else {
      console.log("Received message:", mess);
    }


    channel.ack(msg);
  }
}, { noAck: false }); 

client.on('error', err => console.log('Redis Client Error', err));

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.json());
app.use(cors());


app.get("/", (req, res) => {
  res.json({ message: "Server is running" });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", uptime: process.uptime() });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: "Not found" });
});

// Error handler
app.use(async  (err, req, res, next) => {

    await connectRabbitMQ();
  console.error(err.stack);
  res.status(500).json({ error: "Internal server error" });
});

app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));