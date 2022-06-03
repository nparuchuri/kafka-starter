package com.nparuchuri.kafka.starter.config;

public class ClusterConfigException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -792491682336529678L;

	public ClusterConfigException() {
		super();
	}

	public ClusterConfigException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ClusterConfigException(String message, Throwable cause) {
		super(message, cause);
	}

	public ClusterConfigException(String message) {
		super(message);
	}

	public ClusterConfigException(Throwable cause) {
		super(cause);
	}

}
