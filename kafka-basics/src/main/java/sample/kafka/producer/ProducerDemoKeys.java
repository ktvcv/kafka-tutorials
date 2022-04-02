package sample.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static sample.kafka.producer.KafkaProducerFactory.TOPIC;

public class ProducerDemoKeys {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {

        logger.info("Kafka producer started");

        final KafkaProducer<String, String> kafkaProducer = KafkaProducerFactory.getInstance();

        for (int i = 0; i < 10; i++) {

            final String key = "id_" + i;
            final String value = "kafka_demo_" + i;

            final ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(TOPIC, key, value);

            //async
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                // execute every time message is successfully sent or an exception is thrown
                if (exception == null) {
                    logger.info(format("Received new metadata/ \n" +
                            "Topic: %s," +
                            "Key: %s, " +
                            "Partition: %s, " +
                            "Offset: %s, " +
                            "Timestamp %s",
                        metadata.topic(), producerRecord.key(), metadata.partition(),
                        metadata.offset(), metadata.timestamp()
                        )
                    );
                } else {
                    logger.error("Error during execution :", exception);
                }

            });

            // messages go round robin
            /*try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }

        //sync
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
