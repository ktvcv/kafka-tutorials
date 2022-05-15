package wikimedia.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    private final static Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(final KafkaProducer<String, String> producer, final String topic){
        this.kafkaProducer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {

    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(final String s, final MessageEvent messageEvent) {
       logger.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(final String s) {

    }

    @Override
    public void onError(final Throwable throwable) {
        logger.error("Error in stream reading " + throwable.getMessage());
    }
}
