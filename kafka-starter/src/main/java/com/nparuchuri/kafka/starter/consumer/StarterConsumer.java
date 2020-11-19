package com.nparuchuri.kafka.starter.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nparuchuri.kafka.starter.config.ClusterConfigLoader;
import com.nparuchuri.kafka.starter.producer.ProducerException;

/**
 * 
 * @author Narendra
 *
 */
public class StarterConsumer {

	private KafkaConsumer<String, String> consumer;

	private static Logger logger = LogManager.getLogger(StarterConsumer.class);

	/*
	 * 
	 */
	public StarterConsumer(String topicName) throws ProducerException {
		Properties config = ClusterConfigLoader.loadConsumer();
		this.consumer = new KafkaConsumer<String, String>(config);
		this.consumer.subscribe(Arrays.asList(topicName), new StarterConsumerRebalanceListener());
		List<PartitionInfo> partitions = consumer.partitionsFor(topicName);
		logger.info("Consumer Group id " + this.consumer.groupMetadata().groupId());
		logger.info("Consumer generation id " + this.consumer.groupMetadata().generationId());
		logger.info("Consumer groupInstance id " + this.consumer.groupMetadata().groupInstanceId());
		logger.info("all partitions " + partitions);
		logger.info("Partitions assigned " + this.consumer.assignment());
		
	}

	/**
	 * 
	 * @param seekPosition
	 */
	public void startConsume(String seekPosition) {
		
		
		
		if (seekPosition.equalsIgnoreCase("B")) {
			Set<TopicPartition> partitions = this.consumer.assignment();
			while (partitions.size() == 0) {
				logger.info("Partitions not assigned " + partitions);
				try {
					Thread.sleep(1000);
					partitions = this.consumer.assignment();
					ConsumerRecords<String, String> records = this.consumer.poll(Duration.ZERO);
					for (ConsumerRecord<String, String> record : records) {
						logger.debug("while partitions assignment check ... message consumed partision : "
								+ record.partition() + " key: " + record.key() + " value:" + record.value());
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			logger.info("Partitions assigned " + partitions);

			this.consumer.seekToBeginning(partitions);
		}
		
		while (true) {
			ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(10));
			for (ConsumerRecord<String, String> record : records) {
//				try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
				logger.debug("message consumed partision : " + record.partition() + "producer timestamp "
						+ record.timestamp() + "  current timestamp = " + System.currentTimeMillis() + " Delay = "
						+ (System.currentTimeMillis() - record.timestamp()) + " key: " + record.key() + " value:"
						+ record.value());
			}
			consumer.commitSync();
		}
	}

	/**
	 * 
	 */
	public void close() {
		this.consumer.close();
	}
}
