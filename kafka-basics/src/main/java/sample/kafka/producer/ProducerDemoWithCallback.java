package sample.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public class ProducerDemoWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        logger.info("Kafka producer started");

        final KafkaProducer<String, String> kafkaProducer = KafkaProducerFactory.getInstance();

        for (int i = 0; i < 10; i++) {

            final ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "kafka_demo " + i);

            //async
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                // execute every time message is successfully sent or an exception is thrown
                if (exception == null) {
                    logger.info(format("Received new metadata/ \n"
                            + "Topic: %s, Partition: %s, Offset: %s, Timestamp %s",
                        metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()
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
