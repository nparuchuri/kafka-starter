package com.nparuchuri.kafka.starter.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigException;

public class ClusterConfigLoader {
	
	
	public static Properties loadProducer() throws  ConfigException {
		return load("producer.properties");
	}
	
	public static Properties loadConsumer() throws  ConfigException {
		return load("consumer.properties");
	}
	

	private static Properties load(String fileName) {
		Properties config = new Properties();
		ClassLoader classLoader = ClusterConfigLoader.class.getClassLoader();

		try {
			InputStream resourceAsStream = classLoader.getResourceAsStream(fileName);
			config.load(resourceAsStream);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ConfigException(
					"Error loading configuraiton file - " + fileName + ", make sure it is in classpath", e);
		}
		return config;
	}

}
