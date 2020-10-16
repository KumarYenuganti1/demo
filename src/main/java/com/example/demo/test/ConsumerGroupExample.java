package com.example.demo.test;

import com.example.demo.model.DataRecord;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class ConsumerGroupExample {

    private final static int PARTITION_COUNT = 3;
    private final static String TOPIC_NAME = "topic10";
    private final static int MSG_COUNT = 5;
    private static int totalMsgToSend;
    private static AtomicInteger msg_received_counter = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        String[] consumerGroups = new String[3];
        Arrays.fill(consumerGroups, "test-consumer-group1");
        ConsumerGroupExample.run(3, consumerGroups);

    }

    public static void run(int consumerCount, String[] consumerGroups) throws Exception {
        int distinctGroups = new TreeSet<>(Arrays.asList(consumerGroups)).size();
        totalMsgToSend = MSG_COUNT * PARTITION_COUNT * distinctGroups;
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            int finalI = i;
            executorService.execute(() -> startConsumer(consumerId, consumerGroups[finalI]));
        }
        executorService.execute(ConsumerGroupExample::sendMessages);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    private static void startConsumer(String consumerId, String consumerGroup) {
        System.out.printf("starting consumer: %s, group: %s%n", consumerId, consumerGroup);

        final Properties props = new Properties();
        props.put("bootstrap.servers",
				"cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
        //vhttp://cl0a0722.sunlifecorp.com:33737/clusters/vpguhjQhR0aqIAhJqfqi4w/management/topics

        // Add additional properties.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, DataRecord.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, DataRecord> consumer = new KafkaConsumer<String, DataRecord>(props);
        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, DataRecord> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, DataRecord> record : records) {
                msg_received_counter.incrementAndGet();
                System.out.printf("consumer id:%s, partition id= %s, key = %s, value = %s"
                                + ", offset = %s%n",
                        consumerId, record.partition(),record.key(), record.value(), record.offset());
            }

            consumer.commitSync();
            if(msg_received_counter.get()== totalMsgToSend){
                break;
            }
        }
    }

    //producer - 1 msg topic -- n consumers -  3 consuer -- 1 sonuser
    //producer - 1 msg -- n partitions -- n consuer

    private static void sendMessages() {
//        Properties producerProps = ExampleConfig.getProducerProps();
        final Properties props = new Properties();
        props.put("bootstrap.servers",
				"cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
        
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
//        props.put("log.retention.ms", 1200);
//        props.put("log.retention.bytes", 1);

//        KafkaProducer producer = new KafkaProducer<>(props);
        Producer<String, DataRecord> producer = new KafkaProducer<String, DataRecord>(props);
        int key = 0;
        for (int i = 0; i < MSG_COUNT; i++) {
            DataRecord record = new DataRecord(Long.valueOf(i));
            for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                String value = "message-" + i;
                key++;
                System.out.printf("Sending message topic: %s, key: %s, value: %s, partition id: %s%n",
                        TOPIC_NAME, key, value, partitionId);
//                producer.send(new ProducerRecord<String, DataRecord>(TOPIC_NAME, partitionId,
//                        Integer.toString(key), record));

                producer.send(new ProducerRecord<String, DataRecord>(TOPIC_NAME, partitionId, String.valueOf(key), record), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata m, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        } else {
                            System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                        }
                    }
                });
            }
        }
    }
}
