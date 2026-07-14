import { Kafka } from 'kafkajs';

const config = {
  clientId: process.env.KAFKA_CLIENT_ID ?? 'battlecaos-gateway',
  brokers:  (process.env.KAFKA_BROKER ?? 'localhost:9092').split(',').map((b) => b.trim()), // acepta lista: b1:9092,b2:9093
};

if (process.env.KAFKA_USERNAME) {
  config.ssl  = true;
  config.sasl = {
    mechanism: 'scram-sha-256',
    username:  process.env.KAFKA_USERNAME,
    password:  process.env.KAFKA_PASSWORD,
  };
}

const kafka = new Kafka(config);

export const producer     = kafka.producer();
export const createConsumer = (groupId) => kafka.consumer({ groupId });
