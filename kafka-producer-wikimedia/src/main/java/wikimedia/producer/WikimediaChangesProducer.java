package wikimedia.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.protocol.types.Field;
import sample.kafka.producer.KafkaProducerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final String topicName = "wikimedia.recentchange";

    public static void main(String[] args) throws InterruptedException {

        final KafkaProducer<String, String> kafkaProducer = KafkaProducerFactory.getInstance();
        final EventHandler handler = new WikimediaChangeHandler(kafkaProducer, topicName);
        final String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        final EventSource.Builder eventSourceBuilder = new EventSource.Builder(handler, URI.create(url));
        final EventSource eventSource = eventSourceBuilder.build();

        // start a producer in another thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);


    }
}
