package com.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class OrderCallback implements Callback {

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		System.out.println("onCompletion Topic: "+metadata.topic());
		System.out.println("onCompletion Partition: "+metadata.partition());
		System.out.println("onCompletion offset: "+metadata.offset());
		System.out.println("onCompletion Message Sent Successfully");
		if(exception!=null) {
			exception.printStackTrace();
		}
	
	}

}
