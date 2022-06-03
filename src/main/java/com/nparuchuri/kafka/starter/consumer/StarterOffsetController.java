package com.nparuchuri.kafka.starter.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nparuchuri.kafka.starter.config.ClusterConfigLoader;

public class StarterOffsetController {

	private static Logger logger = LogManager.getLogger(StarterOffsetController.class);

	public static void main(String[] args) {

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter <topic-name> <seek position MM/dd/yyyy/HH:mm:ss > --> ");
		String command = null;
		try {
			command = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String[] params = command.split("\\s+");

		String t = "tp2";
		if (params.length > 1) {
			t = params[0];
		}
		final String topicName = t;
		String seektimeStr = null;
		if (params.length > 1) {
			seektimeStr = params[1];
		}

		Date d = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy/HH:mm:ss");
			d = sdf.parse(seektimeStr);
		} catch (ParseException e) {
			logger.error("Invalid date and time format ", e);
			return;
		}
		
		logger.info("Changing the offets to the timestamp " + d);
		
		final long seekTime = d.getTime();

		Properties config = ClusterConfigLoader.loadConsumer();
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(config);
		
		List<PartitionInfo> partitions = consumer.partitionsFor(topicName);

		logger.info("partitions and offsets " + partitions);

		List<TopicPartition> topicPartitions = partitions.stream()
				.map(p -> new TopicPartition(new String(topicName), p.partition())).collect(Collectors.toList());
		
		
		consumer.assign(topicPartitions);
		
		Map<TopicPartition, Long> partitionTimestampMap = topicPartitions.stream()
		        .collect(Collectors.toMap(tp -> tp, tp -> seekTime));
		
		
		Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap = consumer.offsetsForTimes(partitionTimestampMap);
		
		partitionOffsetMap.forEach((tp, offsetAndTimestamp) -> {
			if (  offsetAndTimestamp !=null ) {
				logger.info("updating offset " + tp);
				consumer.seek(tp, offsetAndTimestamp.offset());
			}
			else {
				logger.warn("Offset Timestamp is null, timestamp may be too old " + tp );
			}
			
		}  );
		
		partitions = consumer.partitionsFor(topicName);

		logger.info("new partitions and offsets " + partitions);
		
		consumer.close();
		
	}

}
