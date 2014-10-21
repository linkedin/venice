package com.linkedin.venice.kafka.consumer;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.linkedin.venice.server.VeniceConfig;

/**
 * Config class for SimpleKafkaConsumer on Storage node.
 */

public class SimpleKafkaConsumerConfig {
	
	// Seed kafka brokers
    private List<String> seedBrokers;
    // SimpleConsumer fetch buffer size.
    private int fetchBufferSize;
    // SimpleConsumer socket timeout.
    private int socketTimeoutMs;
    // Number of times the SimpleConsumer will retry fetching topic-partition leadership metadata.
    private int numMetadataRefreshRetries;
    // Back off duration between metadata fetch retries.
    private int metadataRefreshBackoffMs;
    
    // Default constructor
    public SimpleKafkaConsumerConfig() {
    	this.seedBrokers = Collections.<String>emptyList();
    	this.fetchBufferSize = 64 * 1024;
    	this.socketTimeoutMs = 100;
    	this.numMetadataRefreshRetries = 3;
    	this.metadataRefreshBackoffMs = 10;
    }
  
	public int getFetchBufferSize() {
		return fetchBufferSize;
	}

	public void setFetchBufferSize(int fetchBufferSize) {
		this.fetchBufferSize = fetchBufferSize;
	}

	public int getSocketTimeoutMs() {
		return socketTimeoutMs;
	}

	public void setSocketTimeoutMs(int socketTimeoutMs) {
		this.socketTimeoutMs = socketTimeoutMs;
	}

	public int getNumMetadataRefreshRetries() {
		return numMetadataRefreshRetries;
	}

	public void setNumMetadataRefreshRetries(int numMetadataRefreshRetries) {
		this.numMetadataRefreshRetries = numMetadataRefreshRetries;
	}

	public int getMetadataRefreshBackoffMs() {
		return metadataRefreshBackoffMs;
	}

	public void setMetadataRefreshBackoffMs(int metadataRefreshBackoffMs) {
		this.metadataRefreshBackoffMs = metadataRefreshBackoffMs;
	}
	
	public List<String> getSeedBrokers() {
	  return seedBrokers;
	}

	public void setSeedBrokers(List<String> seedBrokers) {
	  this.seedBrokers = seedBrokers;
	}


	public void sanityCheck() {
      Preconditions.checkArgument(!seedBrokers.isEmpty(), "Kafka seed brokers cannot be empty");
      Preconditions
          .checkArgument(fetchBufferSize > 0, "FetchBufferSize must be positive, value supplied = " + fetchBufferSize);
      Preconditions
          .checkArgument(socketTimeoutMs > 0, "ClientTimeoutMs must be positive, value supplied = " + socketTimeoutMs);
      Preconditions.checkArgument(numMetadataRefreshRetries > 0,
          "NumMetadataRefreshRetries must be positive, value supplied = " + numMetadataRefreshRetries);
    }
}