package com.nparuchuri.kafka.starter.producer;

public class ProducerException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9088032041017714710L;

	public ProducerException() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ProducerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ProducerException(String message, Throwable cause) {
		super(message, cause);
	}

	public ProducerException(String message) {
		super(message);
	}

	public ProducerException(Throwable cause) {
		super(cause);
	}

}
