package com.packt.kafka.wordcount;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamWordCount {
    public static void main(String[] args) throws Exception {
        Properties kafkaStreamProperties = new Properties();
        kafkaStreamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-wordCount");
        kafkaStreamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaStreamProperties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        kafkaStreamProperties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaStreamProperties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder streamTopology = new KStreamBuilder();
        KStream<String, String> topicRecords = streamTopology.stream(stringSerde, stringSerde, "input");
        KStream<String, Long> wordCounts = topicRecords
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .map((key, word) -> new KeyValue<>(word, word))
                .countByKey("Count")
                .toStream();
        wordCounts.to(stringSerde, longSerde, "wordCount");

        KafkaStreams streamManager = new KafkaStreams(streamTopology, kafkaStreamProperties);
        streamManager.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streamManager::close));
    }

}
