const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

mongoose.connect('mongodb://localhost/kafka-sample', { useNewUrlParser: true, useUnifiedTopology: true });

const userSchema = new mongoose.Schema({
  _id: String,
  visitCounter: Number,
  appOpenCounter: Number
});

const User = mongoose.model('User', userSchema);

async function processMessage(message) {
  const userId = message.key.toString();
  const visitCounter = parseInt(message.value.toString());

  // update the visit count in MongoDB
  try {
    await User.updateOne({ _id: userId }, { visitCounter });
    console.log(`Updated visit count for user ${userId}`);
  } catch (err) {
    console.error(`Error updating visit count for user ${userId}: ${err}`);
  }

  // check if the user needs to sign up for the mobile app
  const user = await User.findOne({ _id: userId });
  if (user.visitCounter >= 100 && !user.mobileAppSignup) {
    // publish a message to the Kafka topic for mobile sign-ups
    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
      topic: 'mobile-signups',
      messages: [
        {
          key: userId,
          value: JSON.stringify({ signup: true })
        }
      ]
    });
    console.log(`Published mobile sign-up message for user ${userId}`);
    await producer.disconnect();

    // update the user's mobileAppSignup field in MongoDB
    try {
      await User.updateOne({ _id: userId }, { mobileAppSignup: true });
    } catch (err) {
      console.error(`Error updating mobileAppSignup for user ${userId}: ${err}`);
    }
  }
}

async function startConsumer() {
  const consumer = kafka.consumer({ groupId: 'group-id' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'visit-counter', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });

      await processMessage(message);
    },
  });
}

startConsumer().catch(console.error);
