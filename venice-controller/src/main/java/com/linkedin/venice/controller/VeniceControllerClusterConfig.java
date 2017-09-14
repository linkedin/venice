package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;

/**
 * Configuration which is sepcific to a Venice cluster used by Venice controller.
 */
public class VeniceControllerClusterConfig {
  private static final Logger logger = Logger.getLogger(VeniceControllerClusterConfig.class);

  private final VeniceProperties props;
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
  private long offLineJobWaitTimeInMilliseconds;
  private boolean enableTopicDeletionForUncompletedJob;
  private Map<String, String> clusterToD2Map;

  /**
   * After server disconnecting for delayToRebalanceMS, helix would trigger the re-balance immediately.
   */
  private long delayToRebalanceMS;
  /**
   * If the replica count smaller than minActiveReplica, helix would trigger the re-balance immediately.
   */
  private int minActiveReplica;

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
      this.props = props;
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
    // If the timeout is longer than 3min, we need to update controller client's timeout as well, otherwise creating version would fail.
    offLineJobWaitTimeInMilliseconds = props.getLong(OFFLINE_JOB_START_TIMEOUT_MS, 120000);
    // By default, disable topic deletion when job failed. Because delete an under replicated topic might cause a Kafka MM issue.
    enableTopicDeletionForUncompletedJob = props.getBoolean(ENABLE_TOPIC_DELETION_FOR_UNCOMPLETED_JOB, false);
    // By default, delayed rebalance is disabled.
    delayToRebalanceMS = props.getLong(DELAY_TO_REBALANCE_MS, 0);
    // By default, the min active replica is replica factor minus one, which means if more than one server failed,
    // helix would trigger re-balance immediately.
    minActiveReplica = props.getInt(MIN_ACTIVE_REPLICA, replicaFactor - 1);
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
    clusterToD2Map = props.getMap(CLUSTER_TO_D2);
  }

  public VeniceProperties getProps() {
    return props;
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

  public long getOffLineJobWaitTimeInMilliseconds() {
    return offLineJobWaitTimeInMilliseconds;
  }

  public boolean isEnableTopicDeletionForUncompletedJob() {
    return enableTopicDeletionForUncompletedJob;
  }

  public long getDelayToRebalanceMS() {
    return delayToRebalanceMS;
  }

  public int getMinActiveReplica() {
    return minActiveReplica;
  }

  /**
   * returns kafka Bootstrap Urls . IF there is more than one url, they are separated by commas.
   */
  public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }

  public Map<String, String> getClusterToD2Map() {
    return clusterToD2Map;
  }
}
