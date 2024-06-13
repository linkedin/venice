package com.linkedin.davinci.config;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_EMPTY_POLL_SLEEP_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_MAX_SIZE_PER_SEC;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_MAX_WAIT_TIME_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_MIN_SIZE_PER_SEC;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_PARTITION_MAX_SIZE_PER_SEC;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_QUOTA_BYTES_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_QUOTA_TIME_WINDOW_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_QUOTA_UNORDERED_BYTES_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.KAFKA_FETCH_QUOTA_UNORDERED_RECORDS_PER_SECOND;
import static com.linkedin.venice.ConfigKeys.KAFKA_READ_CYCLE_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.REFRESH_ATTEMPTS_FOR_ZK_RECONNECT;
import static com.linkedin.venice.ConfigKeys.REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * class that maintains config very specific to a Venice cluster
 */
public class VeniceClusterConfig {
  private static final Logger LOGGER = LogManager.getLogger(VeniceServerConfig.class);

  private final String clusterName;
  private final String zookeeperAddress;
  private final PersistenceType persistenceType;
  private final long kafkaFetchQuotaTimeWindow;
  private final long kafkaFetchQuotaBytesPerSecond;
  private final long kafkaFetchQuotaRecordPerSecond;
  private final long kafkaFetchQuotaUnorderedBytesPerSecond;
  private final long kafkaFetchQuotaUnorderedRecordPerSecond;
  private final int refreshAttemptsForZkReconnect;
  private final long refreshIntervalForZkReconnectInMs;
  private final long kafkaReadCycleDelayMs;
  private final long kafkaEmptyPollSleepMs;
  private final long kafkaFetchMinSizePerSecond;
  private final long kafkaFetchMaxSizePerSecond;
  private final long kafkaFetchMaxTimeMS;
  private final long kafkaFetchPartitionMaxSizePerSecond;
  private final String regionName;
  private final PubSubClusterInfo pubSubClusterInfo;
  private final VeniceProperties clusterProperties;
  private final Optional<SSLConfig> sslConfig;

  public VeniceClusterConfig(VeniceProperties clusterProps, Map<String, Map<String, String>> kafkaClusterMap)
      throws ConfigurationException {
    this.clusterName = clusterProps.getString(CLUSTER_NAME);
    this.zookeeperAddress = clusterProps.getString(ZOOKEEPER_ADDRESS);
    this.pubSubClusterInfo = PubSubClusterInfo.extract(clusterProps, kafkaClusterMap);

    try {
      this.persistenceType =
          PersistenceType.valueOf(clusterProps.getString(PERSISTENCE_TYPE, PersistenceType.IN_MEMORY.toString()));
    } catch (UndefinedPropertyException ex) {
      throw new ConfigurationException("persistence type undefined", ex);
    }

    this.kafkaFetchQuotaTimeWindow =
        clusterProps.getLong(KAFKA_FETCH_QUOTA_TIME_WINDOW_MS, TimeUnit.SECONDS.toMillis(5));
    this.kafkaFetchQuotaBytesPerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_QUOTA_BYTES_PER_SECOND, -1);
    this.kafkaFetchQuotaRecordPerSecond = clusterProps.getLong(KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND, -1);
    this.kafkaFetchQuotaUnorderedBytesPerSecond =
        clusterProps.getSizeInBytes(KAFKA_FETCH_QUOTA_UNORDERED_BYTES_PER_SECOND, -1);
    this.kafkaFetchQuotaUnorderedRecordPerSecond =
        clusterProps.getLong(KAFKA_FETCH_QUOTA_UNORDERED_RECORDS_PER_SECOND, -1);

    this.refreshAttemptsForZkReconnect = clusterProps.getInt(REFRESH_ATTEMPTS_FOR_ZK_RECONNECT, 3);
    this.refreshIntervalForZkReconnectInMs =
        clusterProps.getLong(REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS, TimeUnit.SECONDS.toMillis(10));
    this.kafkaReadCycleDelayMs = clusterProps.getLong(KAFKA_READ_CYCLE_DELAY_MS, 1000);
    this.kafkaEmptyPollSleepMs = clusterProps.getLong(KAFKA_EMPTY_POLL_SLEEP_MS, 0);
    // get fetching related from config or use the kafka default values.
    this.kafkaFetchMinSizePerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_MIN_SIZE_PER_SEC, 1);
    this.kafkaFetchMaxSizePerSecond =
        clusterProps.getSizeInBytes(KAFKA_FETCH_MAX_SIZE_PER_SEC, ConsumerConfig.DEFAULT_FETCH_MAX_BYTES);
    this.kafkaFetchMaxTimeMS = clusterProps.getLong(KAFKA_FETCH_MAX_WAIT_TIME_MS, 500);
    this.kafkaFetchPartitionMaxSizePerSecond = clusterProps
        .getSizeInBytes(KAFKA_FETCH_PARTITION_MAX_SIZE_PER_SEC, ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);

    this.regionName = RegionUtils.getLocalRegionName(clusterProps, false);
    LOGGER.info("Final region name for this node: {}", this.regionName);
    if (this.pubSubClusterInfo.isPubSubSSLEnabled()) {
      this.sslConfig = Optional.of(new SSLConfig(clusterProps));
    } else {
      this.sslConfig = Optional.empty();
    }
    this.clusterProperties = clusterProps;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getZookeeperAddress() {
    return zookeeperAddress;
  }

  public PersistenceType getPersistenceType() {
    return persistenceType;
  }

  public String getKafkaBootstrapServers() {
    return pubSubClusterInfo.getPubSubBootstrapServers();
  }

  public SecurityProtocol getKafkaSecurityProtocol(String kafkaBootstrapUrl) {
    SecurityProtocol clusterSpecificSecurityProtocol =
        pubSubClusterInfo.getPubSubBootstrapUrlToSecurityProtocol().get(kafkaBootstrapUrl);
    return clusterSpecificSecurityProtocol == null
        ? pubSubClusterInfo.getPubSubSecurityProtocol()
        : clusterSpecificSecurityProtocol;
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

  public long getKafkaEmptyPollSleepMs() {
    return kafkaEmptyPollSleepMs;
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

  public long getKafkaFetchQuotaBytesPerSecond() {
    return kafkaFetchQuotaBytesPerSecond;
  }

  public long getKafkaFetchQuotaRecordPerSecond() {
    return kafkaFetchQuotaRecordPerSecond;
  }

  public long getKafkaFetchQuotaUnorderedBytesPerSecond() {
    return kafkaFetchQuotaUnorderedBytesPerSecond;
  }

  public long getKafkaFetchQuotaUnorderedRecordPerSecond() {
    return kafkaFetchQuotaUnorderedRecordPerSecond;
  }

  public String getRegionName() {
    return regionName;
  }

  public Int2ObjectMap<String> getKafkaClusterIdToUrlMap() {
    return pubSubClusterInfo.getPubSubClusterIdToUrlMap();
  }

  public Object2IntMap<String> getKafkaClusterUrlToIdMap() {
    return pubSubClusterInfo.getPubSubClusterUrlToIdMap();
  }

  public Int2ObjectMap<String> getKafkaClusterIdToAliasMap() {
    return pubSubClusterInfo.getPubSubClusterIdToAliasMap();
  }

  public Object2IntMap<String> getKafkaClusterAliasToIdMap() {
    return pubSubClusterInfo.getPubSubClusterAliasToIdMap();
  }

  public Map<String, String> getKafkaClusterUrlToAliasMap() {
    return pubSubClusterInfo.getPubSubClusterUrlToAliasMap();
  }

  /**
   * Used to convert from an alternative Kafka URL to the one used in this server instance. For example, can be used
   * in case of a URL migration, or a security protocol migration (e.g. from PLAINTEXT to SSL).
   */
  public Function<String, String> getKafkaClusterUrlResolver() {
    return this.pubSubClusterInfo.getPubSubClusterUrlResolver();
  }

  public Set<String> getRegionNames() {
    return pubSubClusterInfo.getPubSubClusterAliasToIdMap().keySet();
  }

  public VeniceProperties getClusterProperties() {
    return this.clusterProperties;
  }

  public Map<String, Map<String, String>> getKafkaClusterMap() {
    return pubSubClusterInfo.getPubSubClusterMap();
  }

  public PubSubClusterInfo getPubSubClusterInfo() {
    return pubSubClusterInfo;
  }
}
