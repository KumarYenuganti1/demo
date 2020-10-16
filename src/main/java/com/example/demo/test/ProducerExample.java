
package com.example.demo.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.example.demo.model.PageRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.example.demo.model.DataRecord;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ProducerExample {

	public static void main(String[] args) throws UnknownHostException {

		Properties config = new Properties();
		config.put("client.id", InetAddress.getLocalHost().getHostName());
		//config.put("bootstrap.servers",
		//		"cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
		config.put("bootstrap.servers",
				"cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
		config.put("acks", "all");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
		//final String topic = "test-topic3";
		//final String topic = "topic6";
		//final String topic = "pageviews-2";
		final String topic = "topic13";
		// NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
		// AdminClient adminClient = AdminClient.create(config);
		// adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

		//Producer<String, DataRecord> producer = new KafkaProducer<String, DataRecord>(config);
		Producer<String, PageRecord> producer = new KafkaProducer<String, PageRecord>(config);

		// Produce sample data
		final Long numMessages = 30000L;
		for (Long i = 0L; i < numMessages; i++) {
			String key = "alice";
			//DataRecord record = new DataRecord(i);
			PageRecord record = new PageRecord(i, "User_"+i, "pageid"+i);

			System.out.printf("Producing record: %s\t%s%n", key, record);
			producer.send(new ProducerRecord<String, PageRecord>(topic, key, record), new Callback() {
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

