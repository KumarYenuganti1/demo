package io.confluent.examples.streams;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.concurrent.CountDownLatch;

public class Pipe {

    /**
     * Producer -- topic (post) -- consumer (listen)
     * transformation of messages
     * Producer -- topic -- post message
     * Porucer -- mesage post to topic -- into streams --
     * @param args
     */

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> kStream = builder.stream("streams-plaintext-input");
        kStream.foreach((k, v) -> System.out.println("Hello Key = " + k + " Value = " + v));
        //kStream.peek((k, v) -> System.out.println("Key = " + k + " Value = " + v));
        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println("Starting the stream");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping Stream");
            streams.close();
        }));

    }
}
