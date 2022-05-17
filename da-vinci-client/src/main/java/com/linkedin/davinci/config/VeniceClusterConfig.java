package com.linkedin.davinci.config;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;


/**
 * class that maintains config very specific to a Venice cluster
 */
public class VeniceClusterConfig {
  private static final Logger logger = LogManager.getLogger(VeniceServerConfig.class.getName());

  private String clusterName;
  //TODO: shouldn't the following configs be moved to VeniceServerConfig??
  protected String dataBasePath;
  private String zookeeperAddress;
  private PersistenceType persistenceType;
  private String kafkaBootstrapServers;
  private String kafkaZkAddress;
  private long kafkaFetchQuotaTimeWindow;
  private long kafkaFetchQuotaBytesPerSecond;
  private long kafkaFetchQuotaRecordPerSecond;
  private long kafkaFetchQuotaUnorderedBytesPerSecond;
  private long kafkaFetchQuotaUnorderedRecordPerSecond;
  private int refreshAttemptsForZkReconnect;
  private long refreshIntervalForZkReconnectInMs;
  private long kafkaReadCycleDelayMs;
  private long kafkaEmptyPollSleepMs;
  private long kafkaFetchMinSizePerSecond;
  private long kafkaFetchMaxSizePerSecond;
  private long kafkaFetchMaxTimeMS;
  private long kafkaFetchPartitionMaxSizePerSecond;
  private String regionName;

  private Map<Integer, String> kafkaClusterIdToUrlMap = new HashMap<>();
  private Map<String, Integer> kafkaClusterUrlToIdMap = new HashMap<>();
  private Map<Integer, String> kafkaClusterIdToAliasMap = new HashMap<>();
  private Map<String, Integer> kafkaClusterAliasToIdMap = new HashMap<>();
  private Map<String, String> kafkaClusterUrlToAliasMap = new HashMap<>();
  private final Optional<Map<String, Map<String, String>>> kafkaClusterMap;

  private final VeniceProperties clusterProperties;

  private String kafkaSecurityProtocol;
  // SSL related config
  Optional<SSLConfig> sslConfig;

  public VeniceClusterConfig(VeniceProperties clusterProperties, Optional<Map<String, Map<String, String>>> kafkaClusterMap)
      throws ConfigurationException {
    checkProperties(clusterProperties, kafkaClusterMap);
    this.clusterProperties = clusterProperties;
    this.kafkaClusterMap = kafkaClusterMap;
  }

  /**
   * kafkaClusterMap should look like this
   * <property name="venice.server.kafkaClustersMap">
   *       <map>
   *         <entry key="0">
   *           <map>
   *             <entry key="name" value="ei-ltx1"/>
   *             <entry key="url" value="${venice.kafka.ssl.bootstrap.servers.ei-ltx1}"/>
   *           </map>
   *         </entry>
   *         <entry key="1">
   *           <map>
   *             <entry key="name" value="ei4"/>
   *             <entry key="url" value="${venice.kafka.ssl.bootstrap.servers.ei4}"/>
   *           </map>
   *         </entry>
   *       </map>
   * </property>
   * @param kafkaClusterMap
   */
  private void parseKafkaClusterMap(Map<String, Map<String, String>> kafkaClusterMap) {
    kafkaClusterIdToAliasMap.clear();
    kafkaClusterAliasToIdMap.clear();
    kafkaClusterIdToUrlMap.clear();
    kafkaClusterUrlToIdMap.clear();
    for (Map.Entry<String, Map<String, String>> kafkaCluster: kafkaClusterMap.entrySet()) {
      int clusterId = Integer.parseInt(kafkaCluster.getKey());
      Map<String, String> mappings = kafkaCluster.getValue();

      String alias = mappings.get(KAFKA_CLUSTER_MAP_KEY_NAME);
      if (alias != null) {
        kafkaClusterIdToAliasMap.put(clusterId, alias);
        kafkaClusterAliasToIdMap.put(alias, clusterId);
      }

      String url = mappings.get(KAFKA_CLUSTER_MAP_KEY_URL);
      if (url != null) {
        kafkaClusterIdToUrlMap.put(clusterId, url);
        kafkaClusterUrlToIdMap.put(url, clusterId);
      }
    }
  }

  protected void checkProperties(VeniceProperties clusterProps, Optional<Map<String, Map<String, String>>> kafkaClusterMap) throws ConfigurationException {
    clusterName = clusterProps.getString(CLUSTER_NAME);
    zookeeperAddress = clusterProps.getString(ZOOKEEPER_ADDRESS);

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

    kafkaFetchQuotaTimeWindow = clusterProps.getLong(KAFKA_FETCH_QUOTA_TIME_WINDOW_MS, TimeUnit.SECONDS.toMillis(5));
    kafkaFetchQuotaBytesPerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_QUOTA_BYTES_PER_SECOND, -1);
    kafkaFetchQuotaRecordPerSecond = clusterProps.getLong(KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND, -1);
    kafkaFetchQuotaUnorderedBytesPerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_QUOTA_UNORDERED_BYTES_PER_SECOND, -1);
    kafkaFetchQuotaUnorderedRecordPerSecond = clusterProps.getLong(KAFKA_FETCH_QUOTA_UNORDERED_RECORDS_PER_SECOND, -1);

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
    kafkaEmptyPollSleepMs = clusterProps.getLong(KAFKA_EMPTY_POLL_SLEEP_MS, 0);
    // get fetching related from config or use the kafka default values.
    kafkaFetchMinSizePerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_MIN_SIZE_PER_SEC, 1);
    kafkaFetchMaxSizePerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_MAX_SIZE_PER_SEC, ConsumerConfig.DEFAULT_FETCH_MAX_BYTES);
    kafkaFetchMaxTimeMS = clusterProps.getLong(KAFKA_FETCH_MAX_WAIT_TIME_MS, 500);
    kafkaFetchPartitionMaxSizePerSecond = clusterProps.getSizeInBytes(KAFKA_FETCH_PARTITION_MAX_SIZE_PER_SEC, ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);

    Map<Integer, String> kafkaClusterIdToAliasUrlMap = clusterProps.getIntegerToStringMap(SERVER_KAFKA_CLUSTER_ID_TO_URL, Collections.emptyMap());
    kafkaClusterIdToUrlMap = new HashMap<>();
    kafkaClusterIdToAliasMap = new HashMap<>();
    for (Map.Entry<Integer, String> entry : kafkaClusterIdToAliasUrlMap.entrySet()) {
      String[] items = entry.getValue().split("@");
      if (items.length != 2) {
        throw new ConfigurationException("Invalid kafka cluster url config: " + entry);
      }
      kafkaClusterIdToAliasMap.put(entry.getKey(), items[0]);
      kafkaClusterIdToUrlMap.put(entry.getKey(), items[1]);
    }
    kafkaClusterUrlToIdMap = kafkaClusterIdToUrlMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    kafkaClusterAliasToIdMap = kafkaClusterIdToAliasMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    for (Map.Entry<String, Integer> entry : kafkaClusterUrlToIdMap.entrySet()) {
      String kafkaClusterAlias = kafkaClusterIdToAliasMap.get(entry.getValue());
      kafkaClusterUrlToAliasMap.put(entry.getKey(), kafkaClusterAlias);
    }

    this.regionName = RegionUtils.getLocalRegionName(clusterProps, false);
    logger.info("Final region name for this node: " + this.regionName);

    //  parse the new kafka cluster map config. This must be done after processing the SERVER_KAFKA_CLUSTER_ID_TO_URL config above. This will
    //  help deprecate this config later.
    if (kafkaClusterMap.isPresent()) {
      logger.info("Input kafka cluster mapping: " + kafkaClusterMap.get());
      parseKafkaClusterMap(kafkaClusterMap.get());
    }
    logger.info("Derived kafka cluster mapping: kafkaClusterIdToUrlMap: " + kafkaClusterIdToUrlMap + ", kafkaClusterUrlToIdMap: " + kafkaClusterUrlToIdMap +
        ", kafkaClusterIdToAliasMap: " + kafkaClusterIdToAliasMap +  ", kafkaClusterAliasToIdMap: " + kafkaClusterAliasToIdMap);
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
    return kafkaBootstrapServers;
  }

  public String getKafkaZkAddress() {
    return kafkaZkAddress;
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

  public Map<Integer, String> getKafkaClusterIdToUrlMap() {
    return Collections.unmodifiableMap(kafkaClusterIdToUrlMap);
  }

  public Map<String, Integer> getKafkaClusterUrlToIdMap() {
    return Collections.unmodifiableMap(kafkaClusterUrlToIdMap);
  }

  public Map<Integer, String> getKafkaClusterIdToAliasMap() {
    return Collections.unmodifiableMap(kafkaClusterIdToAliasMap);
  }

  public Map<String, Integer> getKafkaClusterAliasToIdMap() {
    return Collections.unmodifiableMap(kafkaClusterAliasToIdMap);
  }

  public Map<String, String> getKafkaClusterUrlToAliasMap() {
    return Collections.unmodifiableMap(kafkaClusterUrlToAliasMap);
  }

  public Set<String> getRegionNames() {
    return Collections.unmodifiableSet(kafkaClusterAliasToIdMap.keySet());
  }

  public VeniceProperties getClusterProperties() {
    return this.clusterProperties;
  }

  public Optional<Map<String, Map<String, String>>> getKafkaClusterMap() {
    return kafkaClusterMap;
  }
}
