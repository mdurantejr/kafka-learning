package com.durante.kafka.streams.colour;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColourApp {

    public static void main(final String[] args) {
        final Properties config = createConfig();
        // 1: Setup topology
        final StreamsBuilder builder = createTopology();
        // step 2 - we read that topic as a KTable so that updates are read correctly
        final KTable<String, String> usersAndColoursTable = builder.table("user-keys-and-colours");
        // step 3 - we count the occurrences of colours
        final KTable<String, Long> favouriteColours = usersAndColoursTable
                // 4 - we group by colour within the KTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // 5 - we output the results to a Kafka Topic - don't forget the serializers
        favouriteColours.toStream().to("favourite-colour-output", Produced.with(Serdes.String(),Serdes.Long()));

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        // only do this in dev - not in prod
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        // print the topology
        System.out.println(kafkaStreams.toString());
        // close resources before shutting down
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static Properties createConfig() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-java");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return properties;
    }

    private static StreamsBuilder createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        // We create the topic of users keys to colours
        final KStream<String, String> inputStream = builder.stream("favourite-colour-input");
        final KStream<String, String> usersAndColours = inputStream
                // 1 - we ensure that a comma is here as we will split on it
                .filter((k, v) -> v.contains(","))
                // 2 - we select a key that will be the user id (lowercase for safety)
                .selectKey((k, v) -> v.split(",")[0].toLowerCase())
                // 3 - we get the colour from the value (lowercase for safety)
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // 4 - we filter undesired colours (could be a data sanitization step
                .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour));

        usersAndColours.to("user-keys-and-colours");
        return builder;
    }

}
