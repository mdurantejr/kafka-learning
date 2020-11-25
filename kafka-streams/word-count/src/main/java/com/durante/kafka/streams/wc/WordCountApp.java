package com.durante.kafka.streams.wc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(final String[] args) {
        final Properties config = createConfig();
        final StreamsBuilder builder = createTopology();
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        kafkaStreams.start();
        System.out.println(kafkaStreams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static Properties createConfig() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return properties;
    }

    static StreamsBuilder createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> wordCountInput = builder.stream("word-count-input");
        final KTable<String, Long> wordCounts = wordCountInput.mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(lowerCaseTextValue -> Arrays.asList(lowerCaseTextValue.split(" ")))
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count(Materialized.as("Counts"));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        return builder;
    }

}
