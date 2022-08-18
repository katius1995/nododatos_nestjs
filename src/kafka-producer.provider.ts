import { Transport, ClientProxyFactory } from '@nestjs/microservices';
import { Producer } from 'kafkajs';
const brokerURLs = ['localhost:9092'];

export const KafkaProducerProvider = {
  provide: 'KafkaProducer',
  useFactory: (): Promise<Producer> => {
    const kafkaClient = ClientProxyFactory.create({
      transport: Transport.KAFKA,
      options: {
        client: {
          brokers: brokerURLs,
        },
        producer: {
          allowAutoTopicCreation: true,
        },
        consumer: {
          groupId: 'nestjs_nododatos',
        },
      },
    });

    return kafkaClient.connect();
  },
};
