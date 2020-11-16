package com.durante.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(final String[] args) {
        LOGGER.info("Setup.");
        final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        final TwitterProducer twitterProducer = new TwitterProducer();
        // create twitter client
        final Client twitterClient = twitterProducer.createTwitterClient(msgQueue);
        twitterClient.connect();

        // create kafka producer
        final KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Stopping application...");
            LOGGER.info("Shutting down client from twitter...");
            twitterClient.stop();
            LOGGER.info("Closing producer...");
            kafkaProducer.close();
            LOGGER.info("done!");
        }));

        // loop to send tweets to kafka


        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                twitterClient.stop();
            }
            if (null != msg) {
                LOGGER.info(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), (metadata, e) -> {
                    if (null != e) {
                        LOGGER.error("Something bad happened", e);
                    }
                });
            }
        }
        LOGGER.info("End of application.");
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        // create producer properties
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // If kafka <=1.1, use 1

        // high throughput producer - at the expense of a bit of latency and CPU usage
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32KB batch size

        return new KafkaProducer<>(properties);
    }

    public Client createTwitterClient(final BlockingQueue<String> msgQueue) {
        final String consumerKey = "";
        final String consumerSecret = "";
        final String token = "";
        final String secret = "";

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        final Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        final StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        final List<String> terms = Lists.newArrayList("kafka", "bitcoin", "usa", "politics", "sport", "soccer");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        final Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        final ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue)); // optional: use this if you want to process client events

        return builder.build();
    }

}
