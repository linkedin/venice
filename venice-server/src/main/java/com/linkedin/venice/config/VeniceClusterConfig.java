package com.linkedin.venice.config;

import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.storage.BdbStorageMetadataService;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;


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
  private String zookeeperAddress;
  private PersistenceType persistenceType;
  private String kafkaBootstrapServers;
  private String kafkaZkAddress;
  private long KafkaFetchQuotaBytesPerSecond = 0;
  private long kafkaFetchQuotaTimeWindow;
  private long kafkaFetchQuotaRecordPerSecond;
  private int statusMessageRetryCount;
  private long statusMessageRetryDurationMs ;
  private int offsetManagerLogFileMaxBytes;
  private int refreshAttemptsForZkReconnect;
  private long refreshIntervalForZkReconnectInMs;
  private long kafkaReadCycleDelayMs;
  private long kafkaFetchMinSizePerSecond;
  private long kafkaFetchMaxSizePerSecond;
  private long kafkaFetchMaxTimeMS;
  private long kafkaFetchPartitionMaxSizePerSecond;


  private String kafkaSecurityProtocol;
  // SSL related config
  Optional<SSLConfig> sslConfig;

  public VeniceClusterConfig(VeniceProperties clusterProperties)
      throws ConfigurationException {
    checkProperties(clusterProperties);
  }

  protected void checkProperties(VeniceProperties clusterProps) throws ConfigurationException {
    clusterName = clusterProps.getString(CLUSTER_NAME);

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
    KafkaFetchQuotaBytesPerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_QUOTA_BYTES_PER_SECOND, 0);
    kafkaFetchQuotaRecordPerSecond = clusterProps.getLong(KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND, 0);
    kafkaFetchQuotaTimeWindow = clusterProps.getLong(KAFKA_FETCH_QUOTA_TIME_WINDOW_MS, TimeUnit.SECONDS.toMillis(5)); //5sec
    statusMessageRetryCount = clusterProps.getInt(STATUS_MESSAGE_RETRY_COUNT, 5);
    statusMessageRetryDurationMs = clusterProps.getLong(STATUS_MESSAGE_RETRY_DURATION_MS, 1000l);

    if (clusterProps.containsKey(DEFAULT_STORAGE_QUOTA)) {
      Store.setDefaultStorageQuota(clusterProps.getLong(DEFAULT_STORAGE_QUOTA));
    }
    if (clusterProps.containsKey(DEFAULT_READ_QUOTA)) {
      Store.setDefaultReadQuota(clusterProps.getLong(DEFAULT_READ_QUOTA));
    }

    kafkaSecurityProtocol = clusterProps.getString(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.PLAINTEXT.name());
    if (!KafkaSSLUtils.isKafkaProtocolValid(kafkaSecurityProtocol)) {
      throw new ConfigurationException("Invalid kafka security protocol: " + kafkaSecurityProtocol);
    }
    if (KafkaSSLUtils.isKafkaSSLProtocol(kafkaSecurityProtocol)) {
      sslConfig = Optional.of(new SSLConfig(clusterProps));
    } else {
      sslConfig = Optional.empty();
    }
    refreshAttemptsForZkReconnect = clusterProps.getInt(REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, 3);
    refreshIntervalForZkReconnectInMs =
        clusterProps.getLong(REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS, java.util.concurrent.TimeUnit.SECONDS.toMillis(10));
    kafkaReadCycleDelayMs = clusterProps.getLong(KAFKA_READ_CYCLE_DELAY_MS, 1000);
    // get fetching related from config or use the kafka default values.
    kafkaFetchMinSizePerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_MIN_SIZE_PER_SEC, 1);
    kafkaFetchMaxSizePerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_MAX_SIZE_PER_SEC, ConsumerConfig.DEFAULT_FETCH_MAX_BYTES);
    kafkaFetchMaxTimeMS = clusterProps.getLong(KAFKA_FETCH_MAX_WAIT_TIME_MS, 500);
    kafkaFetchPartitionMaxSizePerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_PARTITION_MAX_SIZE_PER_SEC, ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);
  }

  public int getStatusMessageRetryCount() {
    return statusMessageRetryCount;
  }

  public long getStatusMessageRetryDurationMs() {
    return statusMessageRetryDurationMs;
  }

  public long getKafkaFetchQuotaBytesPerSecond() {
    return KafkaFetchQuotaBytesPerSecond;
  }

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

  public String getKafkaSecurityProtocol() {
    return kafkaSecurityProtocol;
  }

  public Optional<SSLConfig> getSslConfig() {
    return sslConfig;
  }

  public long getRefreshIntervalForZkReconnectInMs() {
    return refreshIntervalForZkReconnectInMs;
  }

  public int getRefreshAttemptsForZkReconnect() {
    return refreshAttemptsForZkReconnect;
  }

  public long getKafkaReadCycleDelayMs() {
    return kafkaReadCycleDelayMs;
  }

  public long getKafkaFetchMinSizePerSecond() {
    return kafkaFetchMinSizePerSecond;
  }

  public long getKafkaFetchMaxSizePerSecond() {
    return kafkaFetchMaxSizePerSecond;
  }

  public long getKafkaFetchMaxTimeMS() {
    return kafkaFetchMaxTimeMS;
  }

  public long getKafkaFetchPartitionMaxSizePerSecond() {
    return kafkaFetchPartitionMaxSizePerSecond;
  }

  public long getKafkaFetchQuotaTimeWindow() {
    return kafkaFetchQuotaTimeWindow;
  }

  public long getKafkaFetchQuotaRecordPerSecond() {
    return kafkaFetchQuotaRecordPerSecond;
  }
}
