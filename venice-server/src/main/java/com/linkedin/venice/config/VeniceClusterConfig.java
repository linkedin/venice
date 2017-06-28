package com.linkedin.venice.config;

import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.storage.BdbStorageMetadataService;
import com.linkedin.venice.utils.VeniceProperties;

import java.io.File;


/**
 * class that maintains config very specific to a Venice cluster
 */
public class VeniceClusterConfig {
  private String clusterName;
  //TODO: shouldn't the following configs be moved to VeniceServerConfig??
  protected String dataBasePath;
  private String offsetManagerType = null;
  private String offsetDatabasePath = null;
  private long offsetManagerFlushIntervalMs;
  private long offsetDatabaseCacheSize;
  private boolean helixEnabled;
  private String zookeeperAddress;
  private PersistenceType persistenceType;
  private String kafkaBootstrapServers;
  private String kafkaZkAddress;
  private long maxKafkaFetchBytesPerSecond = 0;
  private int statusMessageRetryCount;
  private long statusMessageRetryDurationMs ;
  private int offsetManagerLogFileMaxBytes;

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
        System.getProperty("java.io.tmpdir") + File.separator + BdbStorageMetadataService.OFFSETS_STORE_NAME);
    offsetManagerLogFileMaxBytes = clusterProps.getInt(OFFSET_MANAGER_LOG_FILE_MAX_BYTES, 10 * 1024 * 1024); // 10 MB
    offsetManagerFlushIntervalMs = clusterProps.getLong(OFFSET_MANAGER_FLUSH_INTERVAL_MS, 10000); // 10 sec default
    offsetDatabaseCacheSize = clusterProps.getSizeInBytes(OFFSET_DATABASE_CACHE_SIZE, 50 * 1024 * 1024); // 50 MB

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

  public long getOffsetDatabaseCacheSizeInBytes() {
    return offsetDatabaseCacheSize;
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

  public int getOffsetManagerLogFileMaxBytes() {
    return offsetManagerLogFileMaxBytes;
  }
}
