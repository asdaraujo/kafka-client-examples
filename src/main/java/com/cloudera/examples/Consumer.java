package com.cloudera.examples;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.lang.Math;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Date;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringConsumerInterceptor;

public class Consumer {
    final private static String LAG_BAR = "****************************************************************+";
    final private static int POLL_TIMEOUT_MS = 1000;
    private static String bootstrapServer = null;
    private static String topic = null;
    private static String pollIntervalMs = null;
    private static String maxPollRecords = null;
    private static String propsFile = null;

    public static void main(String[] args) {
        bootstrapServer = args[0];
        topic = args[1];
        pollIntervalMs = args[2];
        maxPollRecords = args[3];
        if (args.length > 4) {
            propsFile = args[4];
        }
        KafkaConsumer<Integer, String> consumer = getConsumer("consumer-example");
        KafkaConsumer<Integer, String> adminConsumer = getConsumer("consumer-example-admin");
        ArrayList<String> topics = new ArrayList<String>();
        topics.add(topic);
        consumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT_MS);
                long consumerTime = (new Date()).getTime();
                adminConsumer.assign(records.partitions());
                adminConsumer.seekToEnd(records.partitions());
                int consumedRecords = 0;
                int totalLag = 0;
                long minCreateTime = consumerTime;
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<Integer, String>> partitionRecords = records.records(partition);
                    //for (ConsumerRecord<Integer, String> record : partitionRecords) {
                    //    System.out.println(String.format("%s", record));
                    //}
                    consumedRecords += partitionRecords.size();
                    long lastPartitionOffset = adminConsumer.position(partition);
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    if (consumedRecords > 0) {
                        long createTime = partitionRecords.get(0).timestamp();
                        if (createTime < minCreateTime) {
                            minCreateTime = createTime;
                        }
                    }
                    totalLag += lastPartitionOffset - lastConsumedOffset;
                }
                long latency = consumerTime - minCreateTime;
                int lagBarLength = (int) Math.min(Math.ceil(totalLag / 50), LAG_BAR.length());
                int latencyBarLength = (int) Math.min(Math.ceil(latency / 1000), LAG_BAR.length());
                System.out.println(String.format("Records: %6d, Lag: %6d, Latency: %6d, Lag: %-65s, Latency: %-65s", consumedRecords, totalLag, latency, LAG_BAR.substring(0, Math.min(lagBarLength, LAG_BAR.length())), LAG_BAR.substring(0, Math.min(latencyBarLength, LAG_BAR.length()))));

                //for (ConsumerRecord<Integer, String> record : records) {
                //    System.out.println(String.format("%s", record));
                //}
                consumer.commitSync();
                Thread.sleep(Integer.parseInt(pollIntervalMs));
            }
        } catch (InterruptedException e) {
            System.out.println("Received interrupt signal...");
        } finally {
            System.out.println("Closing consumer");
            consumer.close();
        }
        System.exit(0);
    }
    public static KafkaConsumer<Integer, String> getConsumer(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Math.max(30000, 2 * Integer.parseInt(pollIntervalMs)));
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringConsumerInterceptor");

        if (propsFile != null) {
            try (InputStream input = new FileInputStream(propsFile)) {
                properties.load(input);
                System.out.println(String.format("%s", properties));
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        return new KafkaConsumer<Integer, String>(properties);
    }
}
