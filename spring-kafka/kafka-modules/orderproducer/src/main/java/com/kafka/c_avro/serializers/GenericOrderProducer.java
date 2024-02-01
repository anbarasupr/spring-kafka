package com.kafka.c_avro.serializers;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.bharath.kafka.orderproducer.OrderCallback;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class GenericOrderProducer {

	public static void main(String[] args) throws IOException {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
		props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		props.setProperty("schema.registry.url", "http://localhost:8081");

		KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
		Parser parser = new Schema.Parser();
		Schema schema = parser.parse("{\n"
				+ "\"namespace\": \"com.kafka.avro\",\n"
				+ "\"type\": \"record\",\n"
				+ "\"name\": \"Order\",\n"
				+ "\"fields\": [\n"
				+ "{\"name\": \"customerName\",\"type\":\"string\"},\n"
				+ "{\"name\": \"product\",\"type\":\"string\"},\n"
				+ "{\"name\": \"quantity\",\"type\":\"int\"}\n"
				+ "]\n"
				+ "}");
		// schema = parser.parse(new File("classpath:order.avsc"));
		GenericRecord order = new GenericData.Record(schema);
		order.put("customerName", "Bharath");
		order.put("product", "Mac Book Pro");
		order.put("quantity", 10101);
		
		
		ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("OrderAvroGRTopic", order.get("customerName").toString(), order);

		try {
			producer.send(record, new OrderCallback());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}

	}

}
