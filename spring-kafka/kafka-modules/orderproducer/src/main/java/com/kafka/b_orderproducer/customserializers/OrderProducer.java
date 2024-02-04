package com.kafka.b_orderproducer.customserializers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.OrderCallback;

public class OrderProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "com.kafka.b_orderproducer.customserializers.OrderSerializer");
		//props.setProperty("partitioner.class", VIPPartitioner.class.getName());

		KafkaProducer<String, Order> producer = new KafkaProducer<String, Order>(props);
		Order order = new Order();
		order.setCustomerName("John");
		order.setProduct("IPhone");
		order.setQuantity(1);
		ProducerRecord<String, Order> record = new ProducerRecord<>("OrderCustomSerializedTopic", order.getCustomerName(), order);

		try {
			producer.send(record, new OrderCallback());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}

	}

}
