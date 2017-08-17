package com.packt.storm.ipfrauddetection;


import com.packt.storm.example.StringToWordsSpliterBolt;
import com.packt.storm.example.WordCountCalculatorBolt;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class IPFraudDetectionTopology {

    private static String zkhost, inputTopic, outputTopic, KafkaBroker, consumerGroup;
    private static String metaStoreURI, dbName, tblName;
    private static final Logger logger = Logger.getLogger(IPFraudDetectionTopology.class);

    public static void Intialize(String arg) {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            logger.info("Loading Configuration File for setting up input");
            input = new FileInputStream(arg);
            prop.load(input);
            zkhost = prop.getProperty("zkhost");
            inputTopic = prop.getProperty("inputTopic");
            outputTopic = prop.getProperty("outputTopic");
            KafkaBroker = prop.getProperty("KafkaBroker");
            consumerGroup = prop.getProperty("consumerGroup");
            metaStoreURI = prop.getProperty("metaStoreURI");
            dbName = prop.getProperty("dbName");
            tblName = prop.getProperty("tblName");

        } catch (IOException ex) {
            logger.error("Error While loading configuration file" + ex);

        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    logger.error("Error Closing input stream");

                }
            }
        }

    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        Intialize(args[0]);
        logger.info("Successfully loaded Configuration ");


        BrokerHosts hosts = new ZkHosts(zkhost);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, inputTopic, "/" + KafkaBroker, consumerGroup);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        String[] partNames = {"status_code"};
        String[] colNames = {"date", "request_url", "protocol_type", "status_code"};

        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper().withColumnFields(new Fields(colNames))
                .withPartitionFields(new Fields(partNames));


        HiveOptions hiveOptions;
        //make sure you change batch size and all paramtere according to requirement
        hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper).withTxnsPerBatch(250).withBatchSize(2)
                .withIdleTimeout(10).withCallTimeout(10000000);

        logger.info("Creating Storm Topology");
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("KafkaSpout", kafkaSpout, 1);

        builder.setBolt("frauddetect", new FraudDetectorBolt()).shuffleGrouping("KafkaSpout");
        builder.setBolt("KafkaOutputBolt",
                new IPFraudKafkaBolt(zkhost, "kafka.serializer.StringEncoder", KafkaBroker, outputTopic), 1)
                .shuffleGrouping("frauddetect");

        builder.setBolt("HiveOutputBolt", new IPFraudHiveBolt(), 1).shuffleGrouping("frauddetect");
        builder.setBolt("HiveBolt", new HiveBolt(hiveOptions)).shuffleGrouping("HiveOutputBolt");

        Config conf = new Config();
        if (args != null && args.length > 1) {
            conf.setNumWorkers(3);
            logger.info("Submiting  topology to storm cluster");

            StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
        } else {
            // Cap the maximum number of executors that can be spawned
            // for a component to 3
            conf.setMaxTaskParallelism(3);
            // LocalCluster is used to run locally
            LocalCluster cluster = new LocalCluster();
            logger.info("Submitting  topology to local cluster");
            cluster.submitTopology("KafkaLocal", conf, builder.createTopology());
            // sleep
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                logger.error("Exception ocuured" + e);
                cluster.killTopology("KafkaToplogy");
                logger.info("Shutting down cluster");
                cluster.shutdown();
            }
            cluster.shutdown();

        }

    }
}

