package com.kafka.d_orderproducer.customserializers.partitioners;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class VIPPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		System.out.println("partition topic:" + topic + ", key:" + key + ", keyBytes:" + keyBytes + ", value:" + value
				+ ", valueBytes:" + valueBytes);
		List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
		if (((String) key).equals("Bharath")) {
			return 5;
		}
		return (Math.abs(Utils.murmur2(keyBytes)) % partitions.size() - 1); // murmur2 - hashing algorithm
	}

	@Override
	public void close() {

	}

}
