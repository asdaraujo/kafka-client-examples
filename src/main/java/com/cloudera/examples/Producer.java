package com.cloudera.examples;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor;

public class Producer {
    private static String bootstrapServer = null;
    private static String topic = null;
    private static String produceIntervalMs = null;
    private static int minRecords = 0;
    private static int maxRecords = 0;
    private static double burstsPerMinute = 0;
    private static int minBurstRecords = 0;
    private static int maxBurstRecords = 0;
    private static String propsFile = null;

    private static KafkaProducer<Integer, String> producer = null;
    private static int msgId = 0;

    private static void produceRecords(int numOfRecords) {
        for(int i=0; i<numOfRecords; i++) {
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, msgId, "Message #" + Integer.toString(msgId++));
            producer.send(record);
            //System.out.println(String.format("%s", record));
        }
    }

    public static void main(String[] args) {
        bootstrapServer = args[0];
        topic = args[1];
        produceIntervalMs = args[2];
        minRecords = Integer.parseInt(args[3]);
        maxRecords = Integer.parseInt(args[4]);
        burstsPerMinute = Double.parseDouble(args[5]);
        minBurstRecords = Integer.parseInt(args[6]);
        maxBurstRecords = Integer.parseInt(args[7]);
        producer = getProducer();
        Random random = new Random();
        double threshold = burstsPerMinute * Double.parseDouble(produceIntervalMs) / 60000;
        int cycle = 0;
        try {
            while (true) {
                int numOfRecords = random.nextInt((maxRecords - minRecords) + 1) + minRecords;
                System.out.println(String.format("[%d] Records: %d", cycle++, numOfRecords));
                produceRecords(numOfRecords);

                double d = random.nextDouble();
                if (random.nextDouble() < threshold) {
                    numOfRecords = random.nextInt((maxBurstRecords - minBurstRecords) + 1) + minBurstRecords;
                    System.out.println(String.format("Burst of records: %d", numOfRecords));
                    produceRecords(numOfRecords);
                }
                Thread.sleep(Integer.parseInt(produceIntervalMs));
            }
        } catch (InterruptedException e) {
            System.out.println("Received interrupt signal...");
        } finally {
            System.out.println("Closing producer");
            producer.close();
        }
        System.exit(0);
    }
    public static KafkaProducer<Integer, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-example");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        if (propsFile != null) {
            try (InputStream input = new FileInputStream(propsFile)) {
                properties.load(input);
                System.out.println(String.format("%s", properties));
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        return new KafkaProducer<Integer, String>(properties);
    }
}

