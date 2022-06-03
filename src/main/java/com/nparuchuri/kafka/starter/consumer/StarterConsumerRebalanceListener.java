package com.nparuchuri.kafka.starter.consumer;

import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StarterConsumerRebalanceListener implements ConsumerRebalanceListener {
	
	private static Logger logger = LogManager.getLogger(StarterConsumerRebalanceListener.class);


	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		logger.info("Partitions Revoked" + partitions);
	}

	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		logger.info("Partitions Assigned" + partitions);
	}

	
}
