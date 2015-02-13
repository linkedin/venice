package com.linkedin.venice.config;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import com.linkedin.venice.store.bdb.BdbStoreConfig;
import com.linkedin.venice.store.memory.InMemoryStorageEngineFactory;
import com.linkedin.venice.utils.Props;

import java.util.List;
import java.util.Map;


/**
 * class that maintains all properties that are not specific to a venice server and cluster.
 * Includes individual store properties and other properties that can be overwritten.
 */
public class VeniceStoreConfig extends VeniceServerConfig {
  /* TODO once we have BDB implementation, add BDB type to  storageEngineFactoryClassNameMap like shown below
    storageEngineFactoryClassNameMap = ImmutableMap
    .of("inMemory", InMemoryStorageEngineFactory.class.getName(), "bdb", BdbStorageEngineFactory.class.getName());
*/
  public static final Map<String, String> storageEngineFactoryClassNameMap =
    ImmutableMap.of("inMemory", InMemoryStorageEngineFactory.class.getName(),
      "bdb", BdbStorageEngineFactory.class.getName());

  private String storeName;
  private String persistenceType;
  private int storageReplicationFactor;

  private int numKafkaPartitions;
  private String kafkaZookeeperUrl;
  private List<String> kafkaBrokers;
  // assumes that all kafka brokers listen on the same port
  private int kafkaBrokerPort;
  // SimpleConsumer fetch buffer size.
  private int fetchBufferSize;
  // SimpleConsumer socket timeout.
  private int socketTimeoutMs;
  // Number of times the SimpleConsumer will retry fetching topic-partition leadership metadata.
  private int numMetadataRefreshRetries;
  // Back off duration between metadata fetch retries.
  private int metadataRefreshBackoffMs;
  // TODO: Store level bdb configuration, need to create StoreStorageConfig abstract class and extend from that
  private BdbStoreConfig bdbStoreConfig;

  public VeniceStoreConfig(Props storeProperties)
    throws ConfigurationException {
    super(storeProperties);
    initAndValidateProperties(storeProperties);
  }

  private void initAndValidateProperties(Props storeProperties)
    throws ConfigurationException {
    storeName = storeProperties.getString(VeniceConfigService.STORE_NAME);
    try {
      persistenceType = storeProperties.getString(VeniceConfigService.PERSISTENCE_TYPE);   // Assign a default ?
    } catch (UndefinedPropertyException ex) {
      throw new ConfigurationException("persistence type undefined", ex);
    }
    if (!storageEngineFactoryClassNameMap.containsKey(persistenceType)) {
      throw new ConfigurationException("unknown persistence type: " + persistenceType);
    }
    storageReplicationFactor = storeProperties.getInt(VeniceConfigService.STORAGE_REPLICATION_FACTOR);
    numKafkaPartitions = storeProperties.getInt(VeniceConfigService.NUMBER_OF_KAFKA_PARTITIONS);
    if (numKafkaPartitions < 1) {
      throw new ConfigurationException("num of Kafka partitions cannot be less than 1");
    }
    kafkaZookeeperUrl = storeProperties.getString(VeniceConfigService.KAFKA_ZOOKEEPER_URL);
    if (kafkaZookeeperUrl.isEmpty()) {
      throw new ConfigurationException("kafkaZookeeperUrl can't be empty");
    }
    kafkaBrokers = storeProperties.getList(VeniceConfigService.KAFKA_BROKERS);
    if (kafkaBrokers == null || kafkaBrokers.isEmpty()) {
      throw new ConfigurationException("kafkaBrokers can't be empty");
    }
    kafkaBrokerPort = storeProperties.getInt(VeniceConfigService.KAFKA_BROKER_PORT);
    if (kafkaBrokerPort < 0) {
      throw new ConfigurationException("KafkaBrokerPort can't be negative");
      // TODO additional checks for valid port ?
    }
    fetchBufferSize = storeProperties.getInt(VeniceConfigService.KAFKA_CONSUMER_FETCH_BUFFER_SIZE, 64 * 1024);
    socketTimeoutMs = storeProperties.getInt(VeniceConfigService.KAFKA_CONSUMER_SOCKET_TIMEOUT_MS, 100);
    numMetadataRefreshRetries = storeProperties.getInt(VeniceConfigService.KAFKA_CONSUMER_NUM_METADATA_REFRESH_RETRIES, 3);
    metadataRefreshBackoffMs = storeProperties.getInt(VeniceConfigService.KAFKA_CONSUMER_METADATA_REFRESH_BACKOFF_MS, 1000);

    if (persistenceType.equals("bdb")) {
      bdbStoreConfig = new BdbStoreConfig(storeName, storeProperties);
    } else {
      bdbStoreConfig = null;
    }
    // initialize all other properties here and add getters for the same.

  }

  public String getStoreName() {
    return storeName;
  }

  public String getPersistenceType() {
    return persistenceType;
  }

  public int getStorageReplicationFactor() {
    return storageReplicationFactor;
  }

  public int getNumKafkaPartitions() {
    return numKafkaPartitions;
  }

  public String getKafkaZookeeperUrl() {
    return kafkaZookeeperUrl;
  }

  public List<String> getKafkaBrokers() {
    return kafkaBrokers;
  }

  public int getKafkaBrokerPort() {
    return kafkaBrokerPort;
  }

  public int getFetchBufferSize() {
    return fetchBufferSize;
  }

  public int getSocketTimeoutMs() {
    return socketTimeoutMs;
  }

  public int getNumMetadataRefreshRetries() {
    return numMetadataRefreshRetries;
  }

  public int getMetadataRefreshBackoffMs() {
    return metadataRefreshBackoffMs;
  }

  public String getStorageEngineFactoryClassName() {
    return storageEngineFactoryClassNameMap.get(this.persistenceType);
  }

  public BdbStoreConfig getBdbStoreConfig() {
    return this.bdbStoreConfig;
  }
}
