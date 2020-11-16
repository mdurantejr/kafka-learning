package com.durante.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(final String[] args) {
        final CountDownLatch latch = new CountDownLatch(1);
        final Runnable runnable = new ConsumerDemoWithThread().new ConsumerThread(latch, "first_topic");
        final Thread thread = new Thread(runnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            LOGGER.info("Caught shutdown hook");
            ((ConsumerThread) runnable).shutdown();
            try {
                latch.await();
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("Application has finished.");
        }));

        try {
            latch.wait();
        } catch (final InterruptedException e) {
            LOGGER.error("Application got interrupted.", e);
        } finally {
            LOGGER.info("Application is closing.");
        }
    }

    public class ConsumerThread implements Runnable {

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;

        public ConsumerThread(final CountDownLatch latch, final String topic) {
            this.latch = latch;

            // create properties
            final Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fourth-application");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                // poll for new data
                while (true) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));

                    records.forEach(record -> {
                        LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                        LOGGER.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    });
                }
            } catch (final WakeupException e) {
                LOGGER.info("Received shutdown signal!", e);
            } finally {
                consumer.close();
                // Tell main code we're done with the consumer.
                latch.countDown();
            }
        }

        public void shutdown() {
            // interrupts the consumer.poll(), it'll throw WakeUpException
            consumer.wakeup();
        }
    }

}
