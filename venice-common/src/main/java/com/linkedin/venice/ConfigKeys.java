package com.linkedin.venice;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by mwise on 1/19/16.
 */
public class ConfigKeys {
  private ConfigKeys(){}

  // cluster specific properties
  public static final String CLUSTER_NAME = "cluster.name";
  public static final String STORAGE_NODE_COUNT = "storage.node.count";
  public static final String DATA_BASE_PATH = "data.base.path";
  public static final String PARTITION_NODE_ASSIGNMENT_SCHEME = "partition.node.assignment.scheme";
  public static final String ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT = "enable.kafka.consumers.offset.management";
  public static final String OFFSET_MANAGER_TYPE = "offset.manager.type";
  public static final String OFFSET_DATA_BASE_PATH = "offsets.data.base.path";
  public static final String OFFSET_MANAGER_FLUSH_INTERVAL_MS = "offset.manager.flush.interval.ms";
  public static final String HELIX_ENABLED = "helix.enabled";
  public static final String ZOOKEEPER_ADDRESS = "zookeeper.address";
  public static final Set<String> clusterSpecificProperties = new HashSet<String>(Arrays
      .asList(CLUSTER_NAME, STORAGE_NODE_COUNT, PARTITION_NODE_ASSIGNMENT_SCHEME,
          ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT, OFFSET_MANAGER_TYPE, OFFSET_DATA_BASE_PATH,
          OFFSET_MANAGER_FLUSH_INTERVAL_MS, HELIX_ENABLED, ZOOKEEPER_ADDRESS));
  public static final String ROUTER_PORT = "router.port";

  public static final String LISTENER_PORT = "listener.port";
  public static final String ADMIN_PORT="admin.port";

  // store specific properties
  public static final String STORE_NAME = "store.name";
  public static final String PERSISTENCE_TYPE = "persistence.type";
  public static final String STORAGE_REPLICATION_FACTOR = "storage.node.replicas";
  public static final String NUMBER_OF_KAFKA_PARTITIONS = "kafka.number.partitions";
  public static final String KAFKA_ZOOKEEPER_URL = "kafka.zookeeper.url";
  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_BROKER_PORT = "kafka.broker.port";
  public static final String KAFKA_CONSUMER_FETCH_BUFFER_SIZE = "kafka.consumer.fetch.buffer.size";
  public static final String KAFKA_CONSUMER_SOCKET_TIMEOUT_MS = "kafka.consumer.socket.timeout.ms";
  public static final String KAFKA_CONSUMER_NUM_METADATA_REFRESH_RETRIES =
      "kafka.consumer.num.metadata.refresh.retries";
  public static final String KAFKA_CONSUMER_METADATA_REFRESH_BACKOFF_MS = "kafka.consumer.metadata.refresh.backoff.ms";
  public static final String KAFKA_CONSUMER_ENABLE_AUTO_OFFSET_COMMIT = "kafka.enable.auto.commit";

  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  public static final String KAFKA_AUTO_COMMIT_INTERVAL_MS = "kafka.auto.commit.interval.ms";
  public static final String MAX_KAFKA_FETCH_BYTES_PER_SECOND = "max.kafka.fetch.bytes.per.second";
}
