package com.durante.kafka.streams.wc;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WordCountAppTest {

    private TopologyTestDriver testDriver;

    private final StringSerializer stringSerializer = new StringSerializer();

    private ConsumerRecordFactory<String, String> recordFactory;


    @BeforeEach
    public void setUpTopologyTestDriver(){
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final Topology topology = WordCountApp.createTopology().build();
        testDriver = new TopologyTestDriver(topology, config);
        recordFactory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
    }

    @AfterEach
    public void closeTestDriver(){
        testDriver.close();
    }

    public void pushNewInputRecord(final String value){
        testDriver.pipeInput(recordFactory.create("word-count-input", null, value));
    }

    @Test
    public void dummyTest(){
        final String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }

    public ProducerRecord<String, Long> readOutput(){
        return testDriver.readOutput("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @Test
    public void makeSureCountsAreCorrect(){
        final String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);
        OutputVerifier.compareKeyValue(readOutput(), "testing", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "streams", 1L);
        assertEquals(readOutput(), null);

        final String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        OutputVerifier.compareKeyValue(readOutput(), "testing", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "again", 1L);

    }

    @Test
    public void makeSureWordsBecomeLowercase(){
        final String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 3L);

    }

}