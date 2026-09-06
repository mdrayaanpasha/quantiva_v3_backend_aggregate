import express from "express";
import cors from "cors";
import { createClient } from 'redis';
import dotenv from "dotenv";
import amqplib from "amqplib";


const port = process.env.PORT || 3002;

dotenv.config();

const client = createClient({
  url: process.env.REDIS_KEY
});
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

let Queue = "rsi_queue";            

let conn = await amqplib.connect(rabbitmqUrl);
const channel = await conn.createChannel();

await channel.assertQueue(Queue, { durable: true });
await channel.assertExchange("amq.direct", "direct", { durable: true });

await channel.consume(Queue, async (msg) => {

  if (msg !== null) {       
    const mess = JSON.parse(msg.content.toString());
    console.log("Received message:", mess);

    if (mess.type === "rsi") {
      console.log("here we are...")
      const key = mess.key;

    const analysisData = await client.get(key);
    const candles = JSON.parse(analysisData);

    function calculateRSI(candles, period = 2) {
      let gains = 0;
      let losses = 0;

      for (let i = 1; i < candles.length; i++) {
        const change = candles[i].close - candles[i - 1].close;
        if (change > 0) gains += change;
        else losses -= change; // losses are negative
      }

      const avgGain = gains / period;
      const avgLoss = losses / period;

      if (avgLoss === 0) return 100; // Avoid division by zero

      const rs = avgGain / avgLoss;
      return 100 - (100 / (1 + rs));
    }   

    const rsi = calculateRSI(candles);
        channel.sendToQueue(
        "response_queue",
        Buffer.from(JSON.stringify({"type": "rsi", "result": rsi})),     
        { correlationId: msg.properties.correlationId }  // ← just this line
        );
    channel.ack(msg);
    } else {
      channel.ack(msg);
    }
  }
}, { noAck: false });           


client.on('error', err => console.log('Redis Client Error', err));

const app = express();
const PORT = process.env.PORT || 3004;

app.use(express.json());
app.use(cors());

app.get("/", (req, res) => {
  res.json({ message: "Server is running" });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", uptime: process.uptime() });
});





app.listen(PORT, async() => {
  console.log(`Server running on http://localhost:${PORT}`);
});

