package sample.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static sample.kafka.producer.KafkaProducerFactory.OFFSET_CONFIG;
import static sample.kafka.producer.KafkaProducerFactory.SERVERS;
import static sample.kafka.producer.KafkaProducerFactory.TOPIC;

public class ConsumerDemoWithCooperative {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithCooperative.class.getSimpleName());

    public static void main(String[] args) {

        logger.info("Kafka consumer started");

        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "25");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_CONFIG);
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        final Thread thread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Detected a shutdown, let's exit by calling consumer.wakeUp()...");
            kafkaConsumer.wakeup();

            try {
                thread.join();
            } catch (final InterruptedException exception){
                exception.printStackTrace();
            }
        }));

        try {
            kafkaConsumer.subscribe(singletonList(TOPIC));

            while (true) {
                final ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(1000));

                for (final ConsumerRecord<String, String> record : records) {
                    logger.info("Key : " + record.key() + ", Value : " + record.value());
                    logger.info("Partition : " + record.partition() + ", Offset : " + record.offset());
                }
            }
        } catch (final WakeupException e){
            logger.info("Wake up exception");
        } catch (final Exception e){
            logger.info("Unexpected exception");
        } finally {
            kafkaConsumer.close();
            logger.info("The consumer is now gracefully closed");
        }
    }
}
