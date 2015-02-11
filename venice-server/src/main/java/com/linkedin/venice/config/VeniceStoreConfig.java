package com.linkedin.venice.config;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.store.memory.InMemoryStorageEngineFactory;
import com.linkedin.venice.utils.Props;
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
      ImmutableMap.of("inMemory", InMemoryStorageEngineFactory.class.getName());

  private String storeName;
  private String persistenceType;
  private int storageReplicationFactor;
  private int numKafkaPartitions;
  private String kafkaZookeeperUrl;
  private String kafkaBrokerUrl;
  private int kafkaBrokerPort;

  public VeniceStoreConfig(Props storeProperties)
      throws Exception {
    super(storeProperties);
    initAndValidateProperties(storeProperties);
  }

  private void initAndValidateProperties(Props storeProperties)
      throws Exception {
    storeName = storeProperties.getString(VeniceConfigService.STORE_NAME);
    persistenceType = storeProperties.getString(VeniceConfigService.PERSISTENCE_TYPE);   // Assign a default ?
    if (!storageEngineFactoryClassNameMap.containsKey(persistenceType)) {
      // TODO throw appropriate exception later
      throw new Exception("unknown persistence type: " + persistenceType);
    }
    storageReplicationFactor = storeProperties.getInt(VeniceConfigService.STORAGE_REPLICATION_FACTOR);
    numKafkaPartitions = storeProperties.getInt(VeniceConfigService.NUMBER_OF_KAFKA_PARTITIONS);
    if (numKafkaPartitions < 1) {
      throw new IllegalArgumentException("num of Kafka partitions cannot be less than 1");
    }
    kafkaZookeeperUrl = storeProperties.getString(VeniceConfigService.KAFKA_ZOOKEEPER_URL);
    if (kafkaZookeeperUrl.isEmpty()) {
      throw new IllegalArgumentException("kafkaZookeeperUrl can't be empty");
    }
    kafkaBrokerUrl = storeProperties.getString(VeniceConfigService.KAFKA_BROKER_URL);
    if (kafkaBrokerUrl.isEmpty()) {
      throw new IllegalArgumentException("kafkaBrokerUrl can't be empty");
    }
    kafkaBrokerPort = storeProperties.getInt(VeniceConfigService.KAFKA_BROKER_PORT);

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

  public String getKafkaBrokerUrl() {
    return kafkaBrokerUrl;
  }

  public int getKafkaBrokerPort() {
    return kafkaBrokerPort;
  }

  public String getStorageEngineFactoryClassName() {
    return storageEngineFactoryClassNameMap.get(this.persistenceType);
  }
}
