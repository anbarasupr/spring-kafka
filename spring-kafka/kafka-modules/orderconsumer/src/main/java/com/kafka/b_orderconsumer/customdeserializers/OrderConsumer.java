package com.kafka.b_orderconsumer.customdeserializers;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class OrderConsumer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", StringDeserializer.class.getName());
		props.setProperty("value.deserializer", OrderDeserializer.class.getName());
		props.setProperty("group.id", "OrderCustomSerializedGroup");
		//props.setProperty("auto.commit.interval.ms", "2000");
		//props.setProperty("auto.commit.offset", "false");

		KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("OrderCustomSerializedTopic"));

		ConsumerRecords<String, Integer> orders = consumer.poll(Duration.ofSeconds(50));
		for (ConsumerRecord<String, Integer> order : orders) {
			System.out.println("Product Name " + order.key());
			System.out.println("Quantiy " + order.value());
		}
		consumer.close();
	}

}
