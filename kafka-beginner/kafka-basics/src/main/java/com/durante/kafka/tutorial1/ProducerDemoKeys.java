package com.durante.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(final String[] args) throws ExecutionException, InterruptedException {
        // create producer properties
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            final String key = "id_" + i;
            final String topic = "first_topic";
            final String value = "hello world " + i;
            // producer record
            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            LOGGER.info("Key: " + key);

            // send data - async
            producer.send(record, (metadata, e) -> {
                // success or exception
            if (null == e) {
                // success
                final String log = "Received metadata:"
                        + " \nTopic: " + metadata.topic()
                        + " \nPartition: " + metadata.partition()
                        + " \nOffset: " + metadata.offset()
                        + " \nTimestamp: " + metadata.timestamp();
                LOGGER.info(log);
            } else {
                LOGGER.error("Error while producing", e);
            }
            }).get(); // block the send to make it sync, but don't do it in prod!
        }
        producer.flush();
        producer.close();
    }

}
