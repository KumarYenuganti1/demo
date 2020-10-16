package io.confluent.examples.streams;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
public class AggregatePipe {

    public static void createTopic(final String topic,
                                   final int partitions,
                                   final int replication,
                                   final Properties cloudConfig) {
        //final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        try (final AdminClient admin = AdminClient.create(cloudConfig)) {

            //checking if topic already exists
            boolean alreadyExists = admin.listTopics().names().get().stream()
                    .anyMatch(existingTopicName -> existingTopicName.equals(topic));
            if (alreadyExists) {
                System.out.printf("topic already exits: %s%n", topic);
            } else {
                //creating new topic
                System.out.printf("creating topic: %s%n", topic);
                NewTopic newTopic = new NewTopic(topic, partitions, (short) 1);
                admin.createTopics(Collections.singleton(newTopic)).all().get();
            }
//            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final String output = "streams-words-output";
        //createTopic(output, 1, 1, props);

        //Producer employee record name salary
        //Transfer salary > xxxx -- Consumer need to get names
        //kstream to ktable
        //kstream contains strein employee
        //ktable string string
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> kStream = builder.stream("streams-words-input");

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        final KTable<String, Long> wordCounts = kStream
                // Split each text line, by whitespace, into words.  The text lines are the record
                // values, i.e. we can ignore whatever data is in the record keys and thus invoke
                // `flatMapValues()` instead of the more generic `flatMap()`.
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                // Group the split data by word so that we can subsequently count the occurrences per word.
                // This step re-keys (re-partitions) the input data, with the new record key being the words.
                // Note: No need to specify explicit serdes because the resulting key and value types
                // (String and String) match the application's default serdes.
                .groupBy((keyIgnored, word) -> word)
                // Count the occurrences of each word (record key).
                .count();

        // Write the `KTable<String, Long>` to the output topic.
        wordCounts.toStream().to(output, Produced.with(Serdes.String(), Serdes.Long()));


//        empSalary.toStream().to("streams-employee-output", Produced.with(Serdes.String(), EmployeeRecord.class));

        //Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        //streams.cleanUp();

        //KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println("Starting the stream");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping Stream");
            streams.close();
        }));


    }
}
