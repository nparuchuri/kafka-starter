package com.nparuchuri.kafka.starter.producer.msggen;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * 
 * @author Narendra
 * 
 * Thread safe message generator used to generate with numeric keys as strings and  values as strings
 *
 */
public class RandomMessageGenerator {
	
	private Long counter;
	
	private static RandomMessageGenerator its;
	
	static {
		its = new RandomMessageGenerator();
	}
	
	private RandomMessageGenerator() {
		counter = 0L;
	}
	
	public static RandomMessageGenerator get() {
		return its;
	}
	
	public RandomMessage generateMessage() {
		return new RandomMessage(Long.toString(counter++),RandomStringUtils.randomAscii(1024));
	}

}
