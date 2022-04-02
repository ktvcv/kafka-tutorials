package sample.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static sample.kafka.producer.KafkaProducerFactory.TOPIC;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        logger.info("Kafka producer started");

        final KafkaProducer<String, String> kafkaProducer = KafkaProducerFactory.getInstance();

        final ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>(TOPIC, "kafka_demo");

        //async
        kafkaProducer.send(producerRecord);
        //sync
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
