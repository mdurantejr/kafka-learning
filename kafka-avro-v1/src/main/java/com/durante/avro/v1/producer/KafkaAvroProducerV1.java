package com.durante.avro.v1.producer;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerV1 {

    public static void main(final String[] args) {
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "10");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        final String topic = "customer-avro";
        final Customer customer = Customer.newBuilder()
                .setFirstName("John")
                .setLastName("Doe")
                .setAge(26)
                .setHeight(185.5F)
                .setWeight(85.6F)
                .setAutomatedEmail(false)
                .build();

        final ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(
                topic, customer
        );

        final KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (null == exception) {
                System.out.println("Success!");
                System.out.println(metadata.toString());
            } else {
                exception.printStackTrace();
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }

}