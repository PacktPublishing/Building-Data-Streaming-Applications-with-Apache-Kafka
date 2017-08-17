package com.packt.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;


public class IPLogProducer {
     private File readfile() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("IP_LOG.log").getFile());
           return file;

    }

    public static void main(final String[] args) {
        IPLogProducer ipLogProducer = new IPLogProducer();
        Properties producerProps = new Properties();

        //replace broker ip with your kafka broker ip
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("auto.create.topics.enable","true");

        KafkaProducer<String, String> ipProducer = new KafkaProducer<String, String>(producerProps);

        try (Scanner scanner = new Scanner(ipLogProducer.readfile())) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                ProducerRecord ipData = new ProducerRecord<String, String>("iplog", line);
                Future<RecordMetadata> recordMetadata = ipProducer.send(ipData);
            }
            scanner.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        ipProducer.close();
    }


}


