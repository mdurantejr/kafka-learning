package com.durante.kafka.streams

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}

object FavouriteColourAppScala {

  def main(args: Array[String]): Unit = {
    val config: Properties = new Properties
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-scala")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    val builder: KStreamBuilder = new KStreamBuilder

    // 1: create the topic of users keys to colours
    val textLines: KStream[String, String] = builder.stream("favourite-colour-input")

    val usersAndColours: KStream[String, String] = textLines
      // ensure that a comma is here as we will split on it
      .filter((key: String, value: String) => value.contains(","))
      // select a key that will be the user id (lowecase for safety)
      .selectKey[String]((key: String, value: String) => value.split(",")(0).toLowerCase)
      // get the colour from the value (lowercase for safety)
      .mapValues[String]((value: String) => value.split(",")(1).toLowerCase)
      .filter((user: String, colour: String) => List("green", "blue", "red").contains(colour))

    val intermediaryTopic = "user-keys-and-colours-scala"
    usersAndColours.to(intermediaryTopic)

    // 2: read that topic as a KTable so that updates are read correctly
    val usersAndColoursTable: KTable[String, String] = builder.table(intermediaryTopic)

    // 3: we count the occurrences of colours
    val favouriteColours = usersAndColoursTable
      // group by colour within the KTable
      .groupBy((user: String, colour: String) => new KeyValue[String, String](colour, colour))
      .count("CountsByColours")

    favouriteColours.to(Serdes.String(), Serdes.Long(), "favourite-colour-output-scala")

    val streams: KafkaStreams = new KafkaStreams(builder, config)
    streams.cleanUp()
    streams.start()

    System.out.println(streams.toString)
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = streams.close()
    })

  }

}
