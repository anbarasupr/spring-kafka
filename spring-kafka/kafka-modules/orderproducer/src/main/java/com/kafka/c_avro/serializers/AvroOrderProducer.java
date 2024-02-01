package com.kafka.c_avro.serializers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.avro.Order;
import com.bharath.kafka.orderproducer.OrderCallback;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroOrderProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
		props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		props.setProperty("schema.registry.url", "http://localhost:8081");

		KafkaProducer<String, Order> producer = new KafkaProducer<String, Order>(props);
		Order order = new Order("Bharath","IPhone",3);
		ProducerRecord<String, Order> record = new ProducerRecord<>("OrderAvroTopic", order.getCustomerName().toString(), order);

		try {
			producer.send(record, new OrderCallback());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}

	}

}
