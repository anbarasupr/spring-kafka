package com.kafka.d_orderproducer.customserializers.partitioners;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.kafka.b_orderconsumer.customdeserializers.Order;
import com.kafka.b_orderconsumer.customdeserializers.OrderDeserializer;

public class PartitionedOrderConsumer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", StringDeserializer.class.getName());
		props.setProperty("value.deserializer", OrderDeserializer.class.getName());
		props.setProperty("group.id", "OrderGroup");

		KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("OrderPartitionedTopic"));

		try {
			while (true) {
				ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(20));
				for (ConsumerRecord<String, Order> record : records) {
					String customerName = record.key();
					Order order = record.value();
					System.out.println("Customer Name: " + customerName);
					System.out.println("Product: " + order.getProduct());
					System.out.println("Quantity: " + order.getQuantity());
					System.out.println("Partion: " + record.partition());
				}
			}
		} finally {
			consumer.close();
		}

	}

}
