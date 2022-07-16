package kafka.consumer;

import com.google.gson.JsonParser;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public final class OpenSearchConsumer {

    final static Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
    public static final String INDEX_NAME = "wikimedia";
    public static final String WRAPPER_OBJECT_NAME = "meta";

    @SneakyThrows
    public static void main(String[] args) {

        //consumer
        final KafkaConsumer<String, String> kafkaConsumer = KafkaConsumerFactory.createKafkaConsumer();

        //create OpenSearch client
        final RestHighLevelClient restClient = OpenSearchClientFactory.createOpenSearchClient();

        try (restClient; kafkaConsumer) {
            if (restClient.indices().exists(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT)) {

                final CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
                restClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("The index has been created");
            } else {
                logger.info("The index already exists");
            }
        }
        kafkaConsumer.subscribe(singletonList(INDEX_NAME));

        while (true) {
            final ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
            final int recordCount = records.count();

            logger.info(format("Received %d records", recordCount));

            for (final ConsumerRecord<String, String> consumerRecord : records) {
                final String id = extractId(consumerRecord.value());
                // send to OpenSearch
                try {
                    final IndexRequest request = new IndexRequest(INDEX_NAME)
                        .source(consumerRecord.value(), XContentType.JSON)
                        .id(id);

                    final IndexResponse response = restClient.index(request, RequestOptions.DEFAULT);

                    logger.info("Records were inserted in OpenSearch " + response.getId());
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // close things
        //  restClient.close();
    }

    private static String extractId(final String json) {
        return JsonParser.parseString(json)
            .getAsJsonObject()
            .get(WRAPPER_OBJECT_NAME)
            .getAsJsonObject()
            .get("id")
            .getAsString();
    }
}
