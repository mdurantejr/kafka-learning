package com.durante.avro.v1.consumer;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumerV1 {

    public static void main(final String[] args) {
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "my-avro-consumer");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader", "true");

        final KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(properties);
        final String topic = "customer-avro";

        consumer.subscribe(Collections.singleton(topic));

        System.out.println("Waiting for data...");

        while (true) {
            final ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(500L));
            for (final ConsumerRecord<String, Customer> record : records) {
                final Customer customer = record.value();
                System.out.println(customer);
            }
            consumer.commitSync();
        }

//        consumer.close();
    }

}
