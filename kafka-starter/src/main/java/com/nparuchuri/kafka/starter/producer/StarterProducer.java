package com.nparuchuri.kafka.starter.producer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nparuchuri.kafka.starter.config.ClusterConfigLoader;

public class StarterProducer {

	private String topicName;

	private KafkaProducer<String, String> producer;

	private static Logger logger = LogManager.getLogger(StarterProducer.class);

	public StarterProducer(String topicName)  {
		this.topicName = topicName;
		Properties config = ClusterConfigLoader.loadProducer();
		producer = new KafkaProducer<String, String>(config);
	}

	public Future<RecordMetadata> asyncSendMessage(final String key, final String value) throws ProducerException {
		
		
		Header h1 = new Header() {
			@Override
			public byte[] value() {
				return "Value1".getBytes();
			}
			
			@Override
			public String key() {
				return "Key1";
			}
		};
		
		final ProducerRecord<String, String> record = new ProducerRecord<>(this.topicName, key, value);
		
		return producer.send(record , new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if ( exception == null ) {
					logger.info("Async send completed" + " Topic:" + metadata.topic() + " Partition:" + metadata.partition() + " Key:" + key );
				}
				else {
					logger.info("Async send failed" + " Topic:" + metadata.topic() + " Partition:" + metadata.partition() + " Key:" + key );
				}
			}
		});
	}

	public Future<RecordMetadata> syncSendMessage(String key, String value) throws ProducerException {
		final ProducerRecord<String, String> record = new ProducerRecord<>(this.topicName, key, value);
		Future<RecordMetadata> future = producer.send(record);
		try {
			RecordMetadata metadata = future.get();
			logger.info("Sync send completed" + " Topic:" + metadata.topic() + " Partition:" + metadata.partition() + " key:" + key);
		} catch (InterruptedException | ExecutionException e) {
			throw new ProducerException("Sync send failed failed ", e);
		}
		return future;
	}

	public void close() {
		this.producer.flush();
		this.producer.close();
	}
}
