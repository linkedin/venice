package com.linkedin.venice.config;

import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.offsets.BdbOffsetManager;
import com.linkedin.venice.utils.VeniceProperties;

import java.io.File;


/**
 * class that maintains config very specific to a Venice cluster
 */
public class VeniceClusterConfig {
  private String clusterName;
  protected String dataBasePath;
  private String offsetManagerType = null;
  private String offsetDatabasePath = null;
  private long offsetManagerFlushIntervalMs;


  private boolean helixEnabled;
  private String zookeeperAddress;

  private PersistenceType persistenceType;

  // TODO : All these properties must be passed on to the simple consumer.
  // SimpleConsumer fetch buffer size.
  private int fetchBufferSize;
  // SimpleConsumer socket timeout.
  private int socketTimeoutMs;
  // Number of times the SimpleConsumer will retry fetching topic-partition leadership metadata.
  private int numMetadataRefreshRetries;
  // Back off duration between metadata fetch retries.
  private int metadataRefreshBackoffMs;


  private String kafkaBootstrapServers;

  private String kafkaZkAddress;

  private long maxKafkaFetchBytesPerSecond = 0;

  private int statusMessageRetryCount;
  private long statusMessageRetryDurationMs ;


  public VeniceClusterConfig(VeniceProperties clusterProperties)
      throws ConfigurationException {
    checkProperties(clusterProperties);
  }

  protected void checkProperties(VeniceProperties clusterProps) throws ConfigurationException {
    clusterName = clusterProps.getString(CLUSTER_NAME);

    helixEnabled = clusterProps.getBoolean(HELIX_ENABLED);
    zookeeperAddress = clusterProps.getString(ZOOKEEPER_ADDRESS);
    offsetManagerType = clusterProps.getString(OFFSET_MANAGER_TYPE, PersistenceType.BDB.toString()); // Default "bdb"
    offsetDatabasePath = clusterProps.getString(OFFSET_DATA_BASE_PATH,
        System.getProperty("java.io.tmpdir") + File.separator + BdbOffsetManager.OFFSETS_STORE_NAME);
    offsetManagerFlushIntervalMs = clusterProps.getLong(OFFSET_MANAGER_FLUSH_INTERVAL_MS, 10000); // 10 sec default

    try {
      persistenceType = PersistenceType.valueOf(clusterProps.getString(PERSISTENCE_TYPE,
          PersistenceType.IN_MEMORY.toString()));
    } catch (UndefinedPropertyException ex) {
      throw new ConfigurationException("persistence type undefined", ex);
    }

    kafkaBootstrapServers = clusterProps.getString(KAFKA_BOOTSTRAP_SERVERS);
    if (kafkaBootstrapServers == null || kafkaBootstrapServers.isEmpty()) {
      throw new ConfigurationException("kafkaBootstrapServers can't be empty");
    }
    kafkaZkAddress = clusterProps.getString(KAFKA_ZK_ADDRESS);
    fetchBufferSize = clusterProps.getInt(KAFKA_CONSUMER_FETCH_BUFFER_SIZE, 64 * 1024);
    socketTimeoutMs = clusterProps.getInt(KAFKA_CONSUMER_SOCKET_TIMEOUT_MS, 1000);
    numMetadataRefreshRetries = clusterProps.getInt(KAFKA_CONSUMER_NUM_METADATA_REFRESH_RETRIES, 3);
    metadataRefreshBackoffMs = clusterProps.getInt(KAFKA_CONSUMER_METADATA_REFRESH_BACKOFF_MS, 1000);
    maxKafkaFetchBytesPerSecond = clusterProps.getSizeInBytes(MAX_KAFKA_FETCH_BYTES_PER_SECOND, 0);
    statusMessageRetryCount = clusterProps.getInt(STATUS_MESSAGE_RETRY_COUNT, 5);
    statusMessageRetryDurationMs = clusterProps.getLong(STATUS_MESSAGE_RETRY_DURATION_MS, 1000l);
  }

  public int getStatusMessageRetryCount() {
    return statusMessageRetryCount;
  }

  public long getStatusMessageRetryDurationMs() {
    return statusMessageRetryDurationMs;
  }

  public long getMaxKafkaFetchBytesPerSecond() { return maxKafkaFetchBytesPerSecond; }

  public String getClusterName() {
    return clusterName;
  }

  public String getOffsetManagerType() {
    return offsetManagerType;
  }

  public String getOffsetDatabasePath() {
    return offsetDatabasePath;
  }

  public long getOffsetManagerFlushIntervalMs() {
    return offsetManagerFlushIntervalMs;
  }

  public boolean isHelixEnabled() {
    return helixEnabled;
  }

  public String getZookeeperAddress() {
    return zookeeperAddress;
  }

  public PersistenceType getPersistenceType() {
    return persistenceType;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public String getKafkaZkAddress() {
    return kafkaZkAddress;
  }
}
