package com.nparuchuri.kafka.starter.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConsumerAPI {

	private static Logger logger = LogManager.getLogger(ConsumerAPI.class);

	private String topicName;

	private int consumerCount;

	private String seekPoistion;

	public ConsumerAPI() {

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter <topic-name> <consumer-count> <seek position(B)> --> ");
		String command = null;
		try {
			command = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String[] params = command.split("\\s+");

		this.topicName = "tp2";
		if (params.length > 1) {
			topicName = params[0];
		}
		this.consumerCount = 3;
		if (params.length > 1) {
			this.consumerCount = Integer.parseInt(params[1]);
		}
		this.seekPoistion = "E";
		if (params.length > 2) {
			this.seekPoistion = params[2];
		}

	}

	public void start() {
		long start = System.currentTimeMillis();

		ExecutorService executer = Executors.newFixedThreadPool(this.consumerCount);
		for (int i = 0; i < this.consumerCount; i++) {
			final StarterConsumer consumer = new StarterConsumer(this.topicName);
//			try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
			executer.execute(new Runnable() {
				
				@Override
				public void run() {
					consumer.startConsume(seekPoistion);
				}
			});
		}
		executer.shutdown();
		try {
			executer.awaitTermination(24, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			logger.fatal("Excecuter await termination interrupted ", e);
		}
		logger.info("Total time taken " + (System.currentTimeMillis() - start));
	}

	public static void main(String[] args) {
		ConsumerAPI api = new ConsumerAPI();
		logger.info(api);
		api.start();

	}

	@Override
	public String toString() {
		return "ConsumerAPI [topicName=" + topicName + ", consumerCount=" + consumerCount + ", seekPoistion="
				+ seekPoistion + "]";
	}

}
