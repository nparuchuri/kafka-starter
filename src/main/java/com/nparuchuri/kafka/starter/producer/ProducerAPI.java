package com.nparuchuri.kafka.starter.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nparuchuri.kafka.starter.producer.msggen.RandomMessage;
import com.nparuchuri.kafka.starter.producer.msggen.RandomMessageGenerator;

/**
 * 
 * @author Narendra
 *
 */
public class ProducerAPI {

	private static Logger logger = LogManager.getLogger(ProducerAPI.class);

	private String topicName = "tp2";

	private long messageCount = 10;

	private int producerCount = 1;

	private boolean sync = false;
	
	private int dealyMilliseconds = 0;

	public ProducerAPI() {

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter <topic-name> <msg-count> <producer-count>  <sync(true/false)> <delay ms> --> ");
		String command = null;
		try {
			command = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String[] params = command.split("\\s+");

		if (params.length > 1) {
			this.topicName = params[0];
		}

		if (params.length > 1) {
			this.messageCount = Long.parseLong(params[1]);
		}

		if (params.length > 2) {
			this.producerCount = Integer.parseInt(params[2]);
		}
		if (params.length > 3) {
			this.sync = Boolean.parseBoolean(params[3]);
		}
		
		if (params.length > 4) {
			this.dealyMilliseconds = Integer.parseInt(params[4]);
		}
		
	}
	
	public static void main(String[] args) {
		ProducerAPI producer = new ProducerAPI();
		logger.info(producer);
		producer.start();
	}

	/**
	 * 
	 */
	public void start() {
		long start = System.currentTimeMillis();
		ExecutorService executer = Executors.newFixedThreadPool(this.producerCount);
		for (int i = 0; i < this.producerCount; i++) {
			final StarterProducer producer = new StarterProducer(this.topicName);
			executer.execute(new Runnable() {
				@Override
				public void run() {
					for (long j = 0; j < messageCount; j++) {
						RandomMessage msg = RandomMessageGenerator.get().generateMessage();
						if ( sync) {
							producer.syncSendMessage(msg.getKey(), msg.getValue());
						} else {
							producer.asyncSendMessage(msg.getKey(), msg.getValue());
							
						}
						try {
							if ( dealyMilliseconds > 0 ) {
								Thread.sleep(dealyMilliseconds);
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					producer.close();
				}
			});
		}
		executer.shutdown();
		try {
			executer.awaitTermination(1, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			logger.fatal("Excecuter await termination interrupted ", e);
		}
		logger.info("Total time taken " + (System.currentTimeMillis() - start));
	}

	@Override
	public String toString() {
		return "ProducerAPI [topicName=" + topicName + ", messageCount=" + messageCount + ", producerCount="
				+ producerCount + ", sync=" + sync + "]";
	}

}
