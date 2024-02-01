package com.kafka.a_simpleconsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class SimpleConsumer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "SimpleGroup");
		
		KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singleton("SimpleTopic"));
		ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofSeconds(50));
		
		
		for(ConsumerRecord<String, Integer> record:records) {
			System.out.println("SimpleConsumer key:"+record.key());
			System.out.println("SimpleConsumer value:"+record.value());
			System.out.println("SimpleConsumer partition:"+record.partition());
			System.out.println("SimpleConsumer offset:"+record.offset());
 		}	
		consumer.close();
	}

}
