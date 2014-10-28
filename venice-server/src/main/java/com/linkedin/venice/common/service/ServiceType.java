package com.linkedin.venice.common.service;
/**
 * Various types of Venice services
 */
public enum ServiceType {
	KAFKA_CONSUMER("kafka-consumer-service"),
	STORAGE("storage-service");
	
    private final String display;

    private ServiceType(String display) {
        this.display = display;
    }

    public String getDisplayName() {
        return this.display;
    }
}
