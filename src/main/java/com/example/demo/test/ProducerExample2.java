package com.example.demo.test;

import com.example.demo.model.DataRecord;
import org.apache.kafka.clients.producer.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class ProducerExample2 {

	public static void main(String[] args) throws UnknownHostException {

		Properties config = new Properties();
		config.put("client.id", InetAddress.getLocalHost().getHostName());
		config.put("bootstrap.servers",
				"cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
		config.put("acks", "all");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
		//final String topic = "test-topic3";
		//final String topic = "topic6";
		final String topic = "topic12";
		// NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
		// AdminClient adminClient = AdminClient.create(config);
		// adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

		Producer<String, DataRecord> producer = new KafkaProducer<String, DataRecord>(config);

		// Produce sample data
		final Long numMessages = 30L;
		for (Long i = 0L; i < numMessages; i++) {
			String key = "alice";
			DataRecord record = new DataRecord(i);

			System.out.printf("Producing record: %s\t%s%n", key, record);
			producer.send(new ProducerRecord<String, DataRecord>(topic, key, record), new Callback() {
				@Override
				public void onCompletion(RecordMetadata m, Exception e) {
					if (e != null) {
						e.printStackTrace();
					} else {
						System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(),
								m.partition(), m.offset());
					}
				}
			});
		}

		// producer.flush();

		System.out.printf("30000 messages were produced to topic %s%n", topic);

		producer.close();

	}

}
