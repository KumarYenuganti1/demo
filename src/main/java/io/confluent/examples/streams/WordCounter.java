/*
package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import java.util.Properties;
import static java.util.Arrays.asList;

public class WordCounter {
    public static void main(String[] args) {
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Serde<String> stringSerde = Serdes.String();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        //StreamsBuilder kStreamBuilder = new StreamsBuilder();
        kStreamBuilder.stream(stringSerde, stringSerde, "streams-plaintext-input")
                .flatMapValues(text -> asList(text.split(" ")))
                .map((key, word) -> new KeyValue<>(word, word))
                .countByKey(stringSerde, "Counts")
                .toStream()
                .map((word, count) -> new KeyValue<>(word, word + ":" + count))
                .to(stringSerde, stringSerde, "streams-wordcount-output");

        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, streamsConfig);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Example-Counter");
        props.put("group.id", "test-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-counter");
        //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
*/
