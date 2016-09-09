package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;

/**
 * Configuration which is sepcific to a Venice cluster used by Venice controller.
 */
public class VeniceControllerClusterConfig {
  private static final Logger logger = Logger.getLogger(VeniceControllerClusterConfig.class);

  private String clusterName;
  private String zkAddress;
  private String controllerName;
  private PersistenceType persistenceType;
  private ReadStrategy readStrategy;
  private OfflinePushStrategy offlinePushStrategy;
  private RoutingStrategy routingStrategy;
  private int replicaFactor;
  private int numberOfPartition;
  private int maxNumberOfPartition;
  private long partitionSize;
  private long offLinejobWaitTimeInMilliseconds;

  /**
   * kafka Bootstrap Urls . IF there is more than one url, they are separated by commas
   */
  private String kafkaBootstrapServers;
  /**
   * Number of replication for each kafka topic. It should be different from venice data replica factor.
   */
  private int kafkaReplicaFactor;
  /**
   * Address of zookeeper that kafka used. It may be different from what Helix used.
   */
  private String kafkaZkAddress;

  public VeniceControllerClusterConfig(VeniceProperties props) {
    try {
      checkProperties(props);
      logger.info("Loaded configuration");
    } catch (Exception e) {
      String errorMessage = "Can not load properties.";
      logger.error(errorMessage);
      throw new VeniceException(errorMessage, e);
    }
  }

  private void checkProperties(VeniceProperties props) {
    clusterName = props.getString(CLUSTER_NAME);
    zkAddress = props.getString(ZOOKEEPER_ADDRESS);
    controllerName = props.getString(CONTROLLER_NAME);
    kafkaZkAddress = props.getString(KAFKA_ZK_ADDRESS);
    kafkaReplicaFactor = props.getInt(KAFKA_REPLICA_FACTOR);
    replicaFactor = props.getInt(DEFAULT_REPLICA_FACTOR);
    numberOfPartition = props.getInt(DEFAULT_NUMBER_OF_PARTITION);
    kafkaBootstrapServers = props.getString(KAFKA_BOOTSTRAP_SERVERS);
    partitionSize = props.getSizeInBytes(DEFAULT_PARTITION_SIZE);
    maxNumberOfPartition = props.getInt(DEFAULT_MAX_NUMBER_OF_PARTITIONS);
    offLinejobWaitTimeInMilliseconds = props.getLong(OFFLINE_JOB_START_TIMEOUT_MS, 15000);

    if (props.containsKey(PERSISTENCE_TYPE)) {
      persistenceType = PersistenceType.valueOf(props.getString(PERSISTENCE_TYPE));
    } else {
      persistenceType = PersistenceType.IN_MEMORY;
    }
    if (props.containsKey(DEFAULT_READ_STRATEGY)) {
      readStrategy = ReadStrategy.valueOf(props.getString(DEFAULT_READ_STRATEGY));
    } else {
      readStrategy = ReadStrategy.ANY_OF_ONLINE;
    }
    if (props.containsKey(DEFAULT_OFFLINE_PUSH_STRATEGY)) {
      offlinePushStrategy = OfflinePushStrategy.valueOf(props.getString(DEFAULT_OFFLINE_PUSH_STRATEGY));
    } else {
      offlinePushStrategy = OfflinePushStrategy.WAIT_ALL_REPLICAS;
    }
    if (props.containsKey(DEFAULT_ROUTING_STRATEGY)) {
      routingStrategy = RoutingStrategy.valueOf(props.getString(DEFAULT_ROUTING_STRATEGY));
    } else {
      routingStrategy = RoutingStrategy.CONSISTENT_HASH;
    }
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getZkAddress() {
    return zkAddress;
  }

  public String getControllerName() {
    return controllerName;
  }

  public String getKafkaZkAddress() {
    return kafkaZkAddress;
  }

  public PersistenceType getPersistenceType() {
    return persistenceType;
  }

  public ReadStrategy getReadStrategy() {
    return readStrategy;
  }

  public OfflinePushStrategy getOfflinePushStrategy() {
    return offlinePushStrategy;
  }

  public RoutingStrategy getRoutingStrategy() {
    return routingStrategy;
  }

  public int getReplicaFactor() {
    return replicaFactor;
  }

  public int getNumberOfPartition() {
    return numberOfPartition;
  }

  public int getKafkaReplicaFactor() {
    return kafkaReplicaFactor;
  }

  public long getPartitionSize() {
    return partitionSize;
  }

  public int getMaxNumberOfPartition() {
    return maxNumberOfPartition;
  }

  public long getOffLinejobWaitTimeInMilliseconds() {
    return offLinejobWaitTimeInMilliseconds;
  }

  /**
   * returns kafka Bootstrap Urls . IF there is more than one url, they are separated by commas.
   */
  public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }

}
