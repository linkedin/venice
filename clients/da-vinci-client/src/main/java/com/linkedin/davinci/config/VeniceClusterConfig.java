package com.linkedin.davinci.config;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_OTHER_URLS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL;
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
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.REFRESH_ATTEMPTS_FOR_ZK_RECONNECT;
import static com.linkedin.venice.ConfigKeys.REFRESH_INTERVAL_FOR_ZK_RECONNECT_MS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
  private final String kafkaBootstrapServers;
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

  /**
   * TODO: Encapsulate all these mappings into a "PubSubServiceRepo" which can hand out "PubSubService" objects
   *       by any of the keys here. Each PubSubService object would contain its URL, cluster ID and other relevant info.
   */
  private final Int2ObjectMap<String> kafkaClusterIdToUrlMap;
  private final Object2IntMap<String> kafkaClusterUrlToIdMap;
  private final Int2ObjectMap<String> kafkaClusterIdToAliasMap;
  private final Object2IntMap<String> kafkaClusterAliasToIdMap;
  private final Map<String, String> kafkaClusterUrlToAliasMap;
  private final Map<String, Map<String, String>> kafkaClusterMap;
  private final Map<String, String> kafkaUrlResolution;
  private final Function<String, String> kafkaClusterUrlResolver;

  private final VeniceProperties clusterProperties;

  private final PubSubSecurityProtocol kafkaSecurityProtocol;
  private final Map<String, PubSubSecurityProtocol> kafkaBootstrapUrlToSecurityProtocol;
  private final Optional<SSLConfig> sslConfig;
  private final PubSubPositionTypeRegistry pubSubPositionTypeRegistry;

  public VeniceClusterConfig(VeniceProperties clusterProps, Map<String, Map<String, String>> kafkaClusterMap)
      throws ConfigurationException {
    this.clusterName = clusterProps.getString(CLUSTER_NAME);
    this.zookeeperAddress = clusterProps.getString(ZOOKEEPER_ADDRESS);
    this.pubSubPositionTypeRegistry = PubSubPositionTypeRegistry.fromPropertiesOrDefault(clusterProps);
    try {
      this.persistenceType =
          PersistenceType.valueOf(clusterProps.getString(PERSISTENCE_TYPE, PersistenceType.IN_MEMORY.toString()));
    } catch (UndefinedPropertyException ex) {
      throw new ConfigurationException("persistence type undefined", ex);
    }
    String baseKafkaBootstrapServers = clusterProps.getString(KAFKA_BOOTSTRAP_SERVERS);
    if (baseKafkaBootstrapServers == null || baseKafkaBootstrapServers.isEmpty()) {
      throw new ConfigurationException("kafkaBootstrapServers can't be empty");
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

    String kafkaSecurityProtocolString =
        clusterProps.getString(KAFKA_SECURITY_PROTOCOL, PubSubSecurityProtocol.PLAINTEXT.name());
    if (!ApacheKafkaUtils.isKafkaProtocolValid(kafkaSecurityProtocolString)) {
      throw new ConfigurationException("Invalid kafka security protocol: " + kafkaSecurityProtocolString);
    }
    this.kafkaSecurityProtocol = PubSubSecurityProtocol.forName(kafkaSecurityProtocolString);

    Int2ObjectMap<String> tmpKafkaClusterIdToUrlMap = new Int2ObjectOpenHashMap<>();
    Object2IntMap<String> tmpKafkaClusterUrlToIdMap = new Object2IntOpenHashMap<>();
    Int2ObjectMap<String> tmpKafkaClusterIdToAliasMap = new Int2ObjectOpenHashMap<>();
    Object2IntMap<String> tmpKafkaClusterAliasToIdMap = new Object2IntOpenHashMap<>();
    Map<String, PubSubSecurityProtocol> tmpKafkaBootstrapUrlToSecurityProtocol = new HashMap<>();
    Map<String, String> tmpKafkaUrlResolution = new HashMap<>();

    boolean foundBaseKafkaUrlInMappingIfItIsPopulated = kafkaClusterMap.isEmpty();
    /**
     * The cluster ID, alias and kafka URL mappings are defined in the service config file
     * so in order to support multiple cluster id mappings we pass them as separated entries
     * for example, we can build a new cluster id with its alias and url
     * <entry key="2">
     *   <map>
     *    <entry key="name" value="region1_sep"/>
     *    <entry key="url" value="${venice.kafka.ssl.bootstrap.servers.region1}_sep"/>
     *   </map>
     * </entry>
     *
     * For the separate incremental push topic feature, we duplicate entries with "_sep" suffix and different cluster id
     * to support two RT topics (regular rt and incremental rt) with different cluster id.
     */

    for (Map.Entry<String, Map<String, String>> kafkaCluster: kafkaClusterMap.entrySet()) {
      int clusterId = Integer.parseInt(kafkaCluster.getKey());
      Map<String, String> mappings = kafkaCluster.getValue();

      String alias = mappings.get(KAFKA_CLUSTER_MAP_KEY_NAME);
      if (alias != null) {
        tmpKafkaClusterIdToAliasMap.put(clusterId, alias);
        tmpKafkaClusterAliasToIdMap.put(alias, clusterId);
      }

      String securityProtocolString = mappings.get(KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL);

      String url = mappings.get(KAFKA_CLUSTER_MAP_KEY_URL);
      if (url != null) {
        tmpKafkaClusterIdToUrlMap.put(clusterId, url);
        tmpKafkaClusterUrlToIdMap.put(url, clusterId);
        tmpKafkaUrlResolution.put(url, url);
        if (securityProtocolString != null) {
          tmpKafkaBootstrapUrlToSecurityProtocol.put(url, PubSubSecurityProtocol.valueOf(securityProtocolString));
        }
      }
      if (baseKafkaBootstrapServers.equals(url)) {
        foundBaseKafkaUrlInMappingIfItIsPopulated = true;
      }

      String otherUrls = mappings.get(KAFKA_CLUSTER_MAP_KEY_OTHER_URLS);
      if (otherUrls != null) {
        String[] otherUrlsList = otherUrls.split(",");
        for (String otherUrl: otherUrlsList) {
          if (baseKafkaBootstrapServers.equals(otherUrl)) {
            foundBaseKafkaUrlInMappingIfItIsPopulated = true;
          }

          tmpKafkaClusterUrlToIdMap.put(otherUrl, clusterId);
          String previousMappingForSameName = tmpKafkaUrlResolution.put(otherUrl, url);
          if (previousMappingForSameName != null) {
            throw new IllegalArgumentException(
                "Alternative URLs must be unique, they cannot map to two different Kafka clusters!");
          }
        }
      }
    }
    if (!foundBaseKafkaUrlInMappingIfItIsPopulated) {
      LOGGER.info(
          "baseKafkaBootstrapServers ({}) not found in Kafka cluster mapping: {}",
          baseKafkaBootstrapServers,
          kafkaClusterMap);
    }

    this.kafkaClusterIdToUrlMap = Int2ObjectMaps.unmodifiable(tmpKafkaClusterIdToUrlMap);
    this.kafkaClusterUrlToIdMap = Object2IntMaps.unmodifiable(tmpKafkaClusterUrlToIdMap);
    this.kafkaClusterIdToAliasMap = Int2ObjectMaps.unmodifiable(tmpKafkaClusterIdToAliasMap);
    this.kafkaClusterAliasToIdMap = Object2IntMaps.unmodifiable(tmpKafkaClusterAliasToIdMap);
    this.kafkaBootstrapUrlToSecurityProtocol = Collections.unmodifiableMap(tmpKafkaBootstrapUrlToSecurityProtocol);
    this.kafkaUrlResolution = Collections.unmodifiableMap(tmpKafkaUrlResolution);
    /**
     * If the {@link kafkaClusterIdToUrlMap} and {@link kafkaClusterUrlToIdMap} are equal in size, then it means
     * that {@link KAFKA_CLUSTER_MAP_KEY_OTHER_URLS} was never specified in the {@link kafkaClusterMap}, in which
     * case, the resolver needs not lookup anything, and it will always return the same input with potentially filtering
     */
    this.kafkaClusterUrlResolver = this.kafkaClusterIdToUrlMap.size() == this.kafkaClusterUrlToIdMap.size()
        ? Utils::resolveKafkaUrlForSepTopic
        : url -> Utils.resolveKafkaUrlForSepTopic(kafkaUrlResolution.getOrDefault(url, url));
    this.kafkaBootstrapServers = this.kafkaClusterUrlResolver.apply(baseKafkaBootstrapServers);
    if (this.kafkaBootstrapServers == null || this.kafkaBootstrapServers.isEmpty()) {
      throw new ConfigurationException("kafkaBootstrapServers can't be empty");
    }

    Map<String, String> tmpKafkaClusterUrlToAliasMap = new HashMap<>();
    for (Object2IntMap.Entry<String> entry: tmpKafkaClusterUrlToIdMap.object2IntEntrySet()) {
      String kafkaClusterAlias = tmpKafkaClusterIdToAliasMap.get(entry.getIntValue());
      tmpKafkaClusterUrlToAliasMap.put(entry.getKey(), kafkaClusterAlias);
    }
    this.kafkaClusterUrlToAliasMap = Collections.unmodifiableMap(tmpKafkaClusterUrlToAliasMap);

    if (!ApacheKafkaUtils.isKafkaProtocolValid(kafkaSecurityProtocolString)) {
      throw new ConfigurationException("Invalid kafka security protocol: " + kafkaSecurityProtocolString);
    }
    if (ApacheKafkaUtils.isKafkaSSLProtocol(kafkaSecurityProtocolString)
        || kafkaBootstrapUrlToSecurityProtocol.containsValue(PubSubSecurityProtocol.SSL)) {
      this.sslConfig = Optional.of(new SSLConfig(clusterProps));
    } else {
      this.sslConfig = Optional.empty();
    }

    LOGGER.info(
        "Derived kafka cluster mapping: kafkaClusterIdToUrlMap: {}, kafkaClusterUrlToIdMap: {}, kafkaClusterIdToAliasMap: {}, kafkaClusterAliasToIdMap: {}",
        tmpKafkaClusterIdToUrlMap,
        tmpKafkaClusterUrlToIdMap,
        tmpKafkaClusterIdToAliasMap,
        tmpKafkaClusterAliasToIdMap);
    this.clusterProperties = clusterProps;
    this.kafkaClusterMap = kafkaClusterMap;
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

  public PubSubSecurityProtocol getKafkaSecurityProtocol(String kafkaBootstrapUrl) {
    PubSubSecurityProtocol clusterSpecificSecurityProtocol = kafkaBootstrapUrlToSecurityProtocol.get(kafkaBootstrapUrl);
    return clusterSpecificSecurityProtocol == null ? kafkaSecurityProtocol : clusterSpecificSecurityProtocol;
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
    return kafkaClusterIdToUrlMap;
  }

  public Object2IntMap<String> getKafkaClusterUrlToIdMap() {
    return kafkaClusterUrlToIdMap;
  }

  public Int2ObjectMap<String> getKafkaClusterIdToAliasMap() {
    return kafkaClusterIdToAliasMap;
  }

  public Object2IntMap<String> getKafkaClusterAliasToIdMap() {
    return kafkaClusterAliasToIdMap;
  }

  public Map<String, String> getKafkaClusterUrlToAliasMap() {
    return kafkaClusterUrlToAliasMap;
  }

  /**
   * Used to convert from an alternative Kafka URL to the one used in this server instance. For example, can be used
   * in case of a URL migration, or a security protocol migration (e.g. from PLAINTEXT to SSL).
   */
  public Function<String, String> getKafkaClusterUrlResolver() {
    return this.kafkaClusterUrlResolver;
  }

  public Set<String> getRegionNames() {
    return kafkaClusterAliasToIdMap.keySet();
  }

  public VeniceProperties getClusterProperties() {
    return this.clusterProperties;
  }

  public Map<String, Map<String, String>> getKafkaClusterMap() {
    return kafkaClusterMap;
  }

  /**
   * @return pubsub position mapper
   */
  public PubSubPositionTypeRegistry getPubSubPositionTypeRegistry() {
    return pubSubPositionTypeRegistry;
  }

  /**
   *  For the separate incremental push topic feature, we need to resolve the cluster id to the original one for monitoring
   *  purposes as the incremental push topic essentially uses the same pubsub clusters as the regular push topic, though
   *  it appears to have a different cluster id
   * @param clusterId
   * @return
   */
  public int getEquivalentKafkaClusterIdForSepTopic(int clusterId) {
    String alias = kafkaClusterIdToAliasMap.get(clusterId);
    if (alias == null || !alias.endsWith(Utils.SEPARATE_TOPIC_SUFFIX)) {
      return clusterId;
    }
    String originalAlias = alias.substring(0, alias.length() - Utils.SEPARATE_TOPIC_SUFFIX.length());
    return kafkaClusterAliasToIdMap.getInt(originalAlias);
  }
}
