package com.packt.storm.example;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class KafkaStormWordCountTopology {
    public static void main(String[] args) throws Exception {

        String zkConnString = "localhost:2181";
        String topic = "words";
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic,
                "wordcountID");
        kafkaSpoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafkaspout", new KafkaSpout(kafkaSpoutConfig));
        topologyBuilder.setBolt("stringsplit", new StringToWordsSpliterBolt()).shuffleGrouping("kafkaspout");
        topologyBuilder.setBolt("counter", new WordCountCalculatorBolt()).shuffleGrouping("stringsplit");

        Config config = new Config();
        config.setDebug(true);
        if (args != null && args.length > 1) {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[1], config, topologyBuilder.createTopology());
        } else {
            // Cap the maximum number of executors that can be spawned
            // for a component to 3
            config.setMaxTaskParallelism(3);
            // LocalCluster is used to run locally
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KafkaLocal", config, topologyBuilder.createTopology());
            // sleep
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                cluster.killTopology("KafkaToplogy");
                cluster.shutdown();
            }

            cluster.shutdown();
        }
    }
}