package com.kafka.f_transactions;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TransactionalOrderProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
		props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-1"); // should be unique across various publisher instance
		//props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
		
		KafkaProducer<String, Integer> producer = new KafkaProducer<String, Integer>(props);
		producer.initTransactions();
		ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTransactionTopic", "Mac Book Pro", 10);
		ProducerRecord<String, Integer> record2 = new ProducerRecord<>("OrderTransactionTopic", "Dell Laptop", 20);

		// A publisher cannot handle multiple transactions at the same time before commiting the current transactions
		try {
			producer.beginTransaction();
			// producer.send(record,new OrderCallback()); // Callback is not required  in transactions. If any failure, have to handle abort transactions
			producer.send(record);
			producer.send(record2);
			producer.commitTransaction();
			
		} catch (Exception e) {
			producer.abortTransaction();
			e.printStackTrace();
		} finally {
			producer.close();
		}

	}

}
