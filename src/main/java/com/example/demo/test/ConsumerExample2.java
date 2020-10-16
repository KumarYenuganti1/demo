package com.example.demo.test;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;

import com.example.demo.model.DataRecord;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;

public class ConsumerExample2 {

	public static void main(String[] args) {

		Properties props = new Properties();	
//		props.put("client.id", InetAddress.getLocalHost().getHostName());
		props.put("bootstrap.servers",
				"cl0a2121.sunlifecorp.com:34500, cl0a2120.sunlifecorp.com:34501, cl0a0723.sunlifecorp.com:34502");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
	    props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, DataRecord.class);
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
	    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		final String topic = "test-topic3";
		
		final Consumer<String, DataRecord> consumer2 = new KafkaConsumer<String, DataRecord>(props);

	    consumer2.subscribe(Arrays.asList(topic));
	    
	    Long total_count = 0L;

	    try {
	      while (true) {
	        ConsumerRecords<String, DataRecord> records = consumer2.poll(100);
	        for (ConsumerRecord<String, DataRecord> record : records) {
	          String key = record.key();
	          DataRecord value = record.value();
	          total_count += value.getCount();
	          System.out.printf("Consumed record with key %s and value %s, and updated total count to %d%n", key, value, total_count);
	        }
	      }
	    } finally {
	      consumer2.close();
	    }
		
	}

}
