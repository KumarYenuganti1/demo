package com.example.demo.test;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;

public class TopicCreator {
    public static void main(String[] args) throws Exception {
        createTopic("topic13", 3);
    }

    private static void createTopic(String topicName, int numPartitions) throws Exception {
        Properties config = new Properties();
//        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ExampleConfig.BROKERS);
//        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        config.put("retension.ms", 12000);
//        config.put("retension.bytes", 14000);
       // config.put(AdminClientConfig.)

        config.put("bootstrap.servers",
				"cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
        AdminClient admin = AdminClient.create(config);

        //checking if topic already exists
        boolean alreadyExists = admin.listTopics().names().get().stream()
                .anyMatch(existingTopicName -> existingTopicName.equals(topicName));
        if (alreadyExists) {
            System.out.printf("topic already exits: %s%n", topicName);
        } else {
            //creating new topic
            System.out.printf("creating topic: %s%n", topicName);
            NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }

        //describing
        System.out.println("-- describing topic --");
        admin.describeTopics(Collections.singleton(topicName)).all().get()
                .forEach((topic, desc) -> {
                    System.out.println("Topic: " + topic);
                    System.out.printf("Partitions: %s, partition ids: %s%n", desc.partitions().size(),
                            desc.partitions()
                                    .stream()
                                    .map(p -> Integer.toString(p.partition()))
                                    .collect(Collectors.joining(",")));
                });

        admin.close();
    }
}
