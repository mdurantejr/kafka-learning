package com.durante.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private static final String HOST_NAME = "localhost";

    public static void main(final String[] args) throws IOException {
        final RestHighLevelClient client = createClient();

        final String topic = "twitter_tweets";
        final KafkaConsumer<String, String> consumer = createConsumer(topic);

        // poll for new data
        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
            final int recordsCount = records.count();
            LOGGER.info("Received " + recordsCount + " records.");
            final BulkRequest bulkRequest = new BulkRequest();
            for (final ConsumerRecord<String, String> record : records) {
                // where we insert data into elastic search

                // 2 strategies for kafka IDs
                // manual unique id:
                // final String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                // twitter feed specific id:
                try {
                    final String id = extractIdFromTweet(record.value());


                    final IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id // making it idempotent
                    ).source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                } catch (final NullPointerException e) {
                    LOGGER.warn("Skipping bad data: " + record.value());
                }
            }
            if (recordsCount > 0) {
                final BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                LOGGER.info("Committing offsets...");
                consumer.commitSync();
                LOGGER.info("Offsets have been committed.");
                try {
                    Thread.sleep(1000L);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // client.close();
    }

    private static String extractIdFromTweet(final String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static RestHighLevelClient createClient() {
        final RestClientBuilder builder = RestClient.builder(
                new HttpHost(HOST_NAME, 9200, "http")
        );
        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(final String topic) {
        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-elasticsearch");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

}
