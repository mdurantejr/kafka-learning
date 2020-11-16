package com.durante.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerDemoAssignSeek {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    public static void main(final String[] args) {
        final String topic = "first_topic";
        // create properties
        final Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message
        final TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(partitionToReadFrom));

        // seek
        final long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        final int numberOfMessagesToRead = 5;
        final AtomicBoolean keepOnReading = new AtomicBoolean(true);
        final AtomicInteger numberOfMessagesReadSoFar = new AtomicInteger(0);

        // poll for new data
        while (keepOnReading.get()) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));

            for (final ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar.incrementAndGet();
                LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                LOGGER.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                if (numberOfMessagesReadSoFar.get() >= numberOfMessagesToRead) {
                    keepOnReading.set(false);
                    break;
                }
            }

            LOGGER.info("Exiting the application.");
        }
    }

}
