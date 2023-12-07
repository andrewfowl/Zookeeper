const { Kafka } = require("kafkajs");
const config = require("./config");

const kafka = new Kafka({
  clientId: "consumer-teste",
  brokers: [process.env.BROKERS],
});

const consumer = kafka.consumer({ groupId: `${config.kafkaTopic}-group` });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: config.kafkaTopic, fromBeginning: true });

  let fileBuffer = [];

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      fileBuffer.push(message.value);

      // Assuming some delimiter or condition to indicate the end of the file
      if (message.value.includes("end-of-file-condition")) {
        // Process the complete file
        const completeFile = Buffer.concat(fileBuffer);
        // ... process the file

        // Reset the buffer for the next file
        fileBuffer = [];
      }
    },
  });
};

run().catch(console.error);
