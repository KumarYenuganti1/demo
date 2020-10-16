package io.confluent.examples.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

public class InputProducer {

    public static void createTopic(final String topic,
                                   final int partitions,
                                   final int replication,
                                   final Properties cloudConfig) {
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(final String[] args) throws IOException {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
        // Add additional properties.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
        //
        props.put("retention.ms", 12000);
        props.put("retention.bytes", 14000);

        final String topic = "streams-words-input";
        createTopic(topic, 1, 1, props);

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        List<String> myList = Arrays.asList("name1","name2");

        // Produce sample data
        final Long numMessages = 25L;
        int count = 0;
        for (Long i = 0L; i < numMessages; i++) {
        //for(String str : myList) {
            String key = "key" +(i);
String str = "name"+(i);
            System.out.printf("Producing record: %s\t%s%n", key, str);
            producer.send(new ProducerRecord<String, String>(topic, key, str), new Callback() {
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

        producer.flush();

        System.out.printf("messages were produced to topic %s%n", topic);

        producer.close();
    }
}
