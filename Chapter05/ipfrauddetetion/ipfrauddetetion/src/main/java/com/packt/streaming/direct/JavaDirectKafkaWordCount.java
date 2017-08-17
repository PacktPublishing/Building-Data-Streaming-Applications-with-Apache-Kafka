package com.packt.streaming.direct;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

public class JavaDirectKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        String brokers = "localhost:9092";
        String topics = "test1";

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("DirectKafkaWordCount");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaConfiguration = new HashMap<>();
        kafkaConfiguration.put("metadata.broker.list", brokers);
        kafkaConfiguration.put("group.id", "stream_test8");
        kafkaConfiguration.put("auto.offset.reset", "smallest");

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaConfiguration,
                topicsSet
        );

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        JavaDStream<String> words = lines.flatMap(
                x ->
                        Arrays.asList(SPACE.split(x)
                ).iterator());

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(

                s -> new Tuple2<>(s, 1)

        )
                .reduceByKey((i1, i2) -> i1 + i2);

        //wordCounts.dstream().saveAsTextFiles("hdfs://10.200.99.197:8020/user/chanchal.singh/wordCounts", "result");
        wordCounts.print();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}