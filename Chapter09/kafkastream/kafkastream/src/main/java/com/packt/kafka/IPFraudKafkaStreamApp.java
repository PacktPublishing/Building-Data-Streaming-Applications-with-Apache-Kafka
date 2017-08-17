
package com.packt.kafka;


import com.packt.kafka.lookup.CacheIPLookup;
import com.packt.kafka.utils.PropertyReader;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class IPFraudKafkaStreamApp {
    private static CacheIPLookup cacheIPLookup = new CacheIPLookup();
    private static PropertyReader propertyReader = new PropertyReader();

    public static void main(String[] args) throws Exception {
        Properties kafkaStreamProperties = new Properties();
        kafkaStreamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "IP-Fraud-Detection");
        kafkaStreamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaStreamProperties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        kafkaStreamProperties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaStreamProperties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Serde<String> stringSerde = Serdes.String();

        KStreamBuilder fraudDetectionTopology = new KStreamBuilder();

        KStream<String, String> ipRecords = fraudDetectionTopology.stream(stringSerde, stringSerde, propertyReader.getPropertyValue("topic"));

        KStream<String, String> fraudIpRecords = ipRecords
                .filter((k, v) -> isFraud(v));

        fraudIpRecords.to(propertyReader.getPropertyValue("output_topic"));

        KafkaStreams streamManager = new KafkaStreams(fraudDetectionTopology, kafkaStreamProperties);
        streamManager.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streamManager::close));
    }

    private static boolean isFraud(String record) {
        String IP = record.split(" ")[0];
        String[] ranges = IP.split("\\.");
        String range = null;
        try {
            range = ranges[0];
        } catch (ArrayIndexOutOfBoundsException ex) {
            //handling here
        }
        return cacheIPLookup.isFraudIP(range);
    }
}

