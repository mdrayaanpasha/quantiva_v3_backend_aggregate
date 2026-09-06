import express from "express";
import cors from "cors";
import { createClient } from 'redis';
import dotenv from "dotenv";
import amqplib from "amqplib";


const port = process.env.PORT || 3003;


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

let Queue = "sma_queue";
let conn = await amqplib.connect(rabbitmqUrl);
const channel = await conn.createChannel();

await channel.assertQueue(Queue, { durable: true });
await channel.assertExchange("amq.direct", "direct", { durable: true });

await channel.consume(Queue, async (msg) => {

    if (msg !== null) {
        const mess = JSON.parse(msg.content.toString());
        console.log("Received message:", mess);

        if (mess.type === "sma") {
            console.log("here we are...")
            const key = mess.key;

            const analysisData = await client.get(key);
            const candles = JSON.parse(analysisData);

            function calculateSMA(candles, period = 3) {
                const slice = candles.slice(-period); // last N candles
                const sum = slice.reduce((acc, c) => acc + c.close, 0);
                return sum / slice.length;
            }

            const sma = calculateSMA(candles);
            
 channel.sendToQueue(
        "response_queue",
        Buffer.from(JSON.stringify({ type: mess.type, result: sma    })),
        { correlationId: msg.properties.correlationId }  // ← just this line
        );        }
        channel.ack(msg);
    }
}, { noAck: false });   


const app = express();

app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => {
  res.json({ status: "ok", uptime: process.uptime() });
});

app.get("/", (req, res) => {
  res.json({ message: "Server is running" });
});

app.listen(port, async () => {
    console.log(`Server running on http://localhost:${port}`);
});