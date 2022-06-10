package sample.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducerFactory {

    public static final String SERVERS = "127.0.0.1:9092";
    public static final String GROUP = "group3";
    public static final String OFFSET_CONFIG = "earliest";
    public static final String TOPIC = "demo_java";

    public static KafkaProducer<String, String> getInstance() {
        final Properties properties = new Properties();

        //set high throughput producer configs

        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(properties);

    }

}
