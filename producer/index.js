require("dotenv").config();
const express = require("express");
const multer = require("multer");
const { Kafka } = require("kafkajs");
const config = require("./config");
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3000;

const kafka = new Kafka({
  clientId: "producer-teste",
  brokers: [process.env.BROKERS],
});

const producer = kafka.producer();
producer.connect().catch(console.error);

const upload = multer({ dest: 'uploads/' }); // Files will be temporarily stored in the 'uploads' folder

app.post("/send-file", upload.single('file'), async (req, res) => {
  try {
    const filePath = req.file.path;

    // Create a readable stream
    const fileStream = fs.createReadStream(filePath);

    fileStream.on('data', async (chunk) => {
      await producer.send({
        topic: config.kafkaTopic,
        messages: [{ value: chunk.toString() }], // Convert chunk to string and send as a message
      });
    });

    fileStream.on('end', () => {
      res.status(200).json({ success: true, message: "File streamed and sent to Kafka" });
      fs.unlinkSync(filePath); // Delete the temporary file
    });

  } catch (error) {
    console.error("Error streaming file to Kafka:", error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.listen(port, () => {
  console.log(`Producer API server running at http://localhost:${port}`);
});
