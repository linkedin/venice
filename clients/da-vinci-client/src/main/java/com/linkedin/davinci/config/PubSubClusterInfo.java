package com.linkedin.davinci.config;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_OTHER_URLS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.KAFKA_SECURITY_PROTOCOL;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.utils.KafkaSSLUtils;
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
import java.util.function.Function;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


final public class PubSubClusterInfo {
  private static final Logger LOGGER = LogManager.getLogger(PubSubClusterInfo.class);

  private final String pubSubBootstrapServers;
  private final Int2ObjectMap<String> pubSubClusterIdToUrlMap;
  private final Object2IntMap<String> pubSubClusterUrlToIdMap;
  private final Int2ObjectMap<String> pubSubClusterIdToAliasMap;
  private final Object2IntMap<String> pubSubClusterAliasToIdMap;
  private final Map<String, String> pubSubClusterUrlToAliasMap;
  private final Map<String, Map<String, String>> pubSubClusterMap;
  private final Function<String, String> pubSubClusterUrlResolver;
  private final Map<String, String> pubSubUrlResolution;
  private final Map<String, SecurityProtocol> pubSubBootstrapUrlToSecurityProtocol;
  private final SecurityProtocol pubSubSecurityProtocol;

  public PubSubClusterInfo(
      String pubSubBootstrapServers,
      Int2ObjectMap<String> pubSubClusterIdToUrlMap,
      Object2IntMap<String> pubSubClusterUrlToIdMap,
      Int2ObjectMap<String> pubSubClusterIdToAliasMap,
      Object2IntMap<String> pubSubClusterAliasToIdMap,
      Map<String, String> pubSubClusterUrlToAliasMap,
      Map<String, Map<String, String>> pubSubClusterMap,
      Map<String, String> pubSubUrlResolution,
      Map<String, SecurityProtocol> pubSubBootstrapUrlToSecurityProtocol,
      SecurityProtocol pubSubSecurityProtocol,
      Function<String, String> pubSubClusterUrlResolver) {
    this.pubSubClusterIdToUrlMap = pubSubClusterIdToUrlMap;
    this.pubSubClusterUrlToIdMap = pubSubClusterUrlToIdMap;
    this.pubSubClusterIdToAliasMap = pubSubClusterIdToAliasMap;
    this.pubSubClusterAliasToIdMap = pubSubClusterAliasToIdMap;
    this.pubSubClusterUrlToAliasMap = pubSubClusterUrlToAliasMap;
    this.pubSubClusterMap = pubSubClusterMap;
    this.pubSubUrlResolution = pubSubUrlResolution;
    this.pubSubBootstrapUrlToSecurityProtocol = pubSubBootstrapUrlToSecurityProtocol;
    this.pubSubSecurityProtocol = pubSubSecurityProtocol;
    this.pubSubBootstrapServers = pubSubBootstrapServers;
    this.pubSubClusterUrlResolver = pubSubClusterUrlResolver;

    LOGGER.info("Derived pubSub cluster mapping: {}", this);
  }

  public String getUrlById(int id) {
    return pubSubClusterIdToUrlMap.get(id);
  }

  public int getIdByUrl(String url) {
    return pubSubClusterUrlToIdMap.getOrDefault(url, -1);
  }

  public String getAliasById(int id) {
    return pubSubClusterIdToAliasMap.get(id);
  }

  public int getIdByAlias(String alias) {
    return pubSubClusterAliasToIdMap.getOrDefault(alias, -1);
  }

  public String getAliasByUrl(String url) {
    return pubSubClusterUrlToAliasMap.get(url);
  }

  public Map<String, String> getClusterInfo(String url) {
    return pubSubClusterMap.get(url);
  }

  public Int2ObjectMap<String> getPubSubClusterIdToUrlMap() {
    return pubSubClusterIdToUrlMap;
  }

  public Object2IntMap<String> getPubSubClusterUrlToIdMap() {
    return pubSubClusterUrlToIdMap;
  }

  public Int2ObjectMap<String> getPubSubClusterIdToAliasMap() {
    return pubSubClusterIdToAliasMap;
  }

  public Object2IntMap<String> getPubSubClusterAliasToIdMap() {
    return pubSubClusterAliasToIdMap;
  }

  public Map<String, String> getPubSubClusterUrlToAliasMap() {
    return pubSubClusterUrlToAliasMap;
  }

  public Map<String, Map<String, String>> getPubSubClusterMap() {
    return pubSubClusterMap;
  }

  public Function<String, String> getPubSubClusterUrlResolver() {
    return pubSubClusterUrlResolver;
  }

  public Map<String, String> getPubSubUrlResolution() {
    return pubSubUrlResolution;
  }

  public Map<String, SecurityProtocol> getPubSubBootstrapUrlToSecurityProtocol() {
    return pubSubBootstrapUrlToSecurityProtocol;
  }

  public SecurityProtocol getPubSubSecurityProtocol() {
    return pubSubSecurityProtocol;
  }

  public String getPubSubBootstrapServers() {
    return pubSubBootstrapServers;
  }

  public boolean isPubSubSSLEnabled() {

    return (KafkaSSLUtils.isKafkaSSLProtocol(pubSubSecurityProtocol)
        || pubSubBootstrapUrlToSecurityProtocol.containsValue(SecurityProtocol.SSL));
  }

  @Override
  public String toString() {
    return "PubSubClusterInfo [" + "pubSubClusterIdToUrlMap: " + pubSubClusterIdToUrlMap + ", pubSubClusterUrlToIdMap: "
        + pubSubClusterUrlToIdMap + ", pubSubClusterIdToAliasMap: " + pubSubClusterIdToAliasMap
        + ", pubSubClusterAliasToIdMap: " + pubSubClusterAliasToIdMap + ", pubSubClusterUrlToAliasMap: "
        + pubSubClusterUrlToAliasMap + ", pubSubClusterMap: " + pubSubClusterMap + ']';
  }

  public static PubSubClusterInfo extract(
      VeniceProperties clusterProps,
      Map<String, Map<String, String>> pubSubClusterMap) {
    String basePubSubBootstrapServers = clusterProps.getString(KAFKA_BOOTSTRAP_SERVERS);
    if (basePubSubBootstrapServers == null || basePubSubBootstrapServers.isEmpty()) {
      throw new ConfigurationException("pubSubBootstrapServers can't be empty");
    }

    String pubSubBootstrapServers;
    Function<String, String> pubSubClusterUrlResolver;
    Int2ObjectMap<String> tmpPubSubClusterIdToUrlMap = new Int2ObjectOpenHashMap<>();
    Object2IntMap<String> tmpPubSubClusterUrlToIdMap = new Object2IntOpenHashMap<>();
    Int2ObjectMap<String> tmpPubSubClusterIdToAliasMap = new Int2ObjectOpenHashMap<>();
    Object2IntMap<String> tmpPubSubClusterAliasToIdMap = new Object2IntOpenHashMap<>();
    Map<String, SecurityProtocol> tmpPubSubBootstrapUrlToSecurityProtocol = new HashMap<>();
    Map<String, String> tmpPubSubUrlResolution = new HashMap<>();

    boolean foundBasePubSubUrlInMappingIfItIsPopulated = pubSubClusterMap.isEmpty();
    for (Map.Entry<String, Map<String, String>> pubSubCluster: pubSubClusterMap.entrySet()) {
      int clusterId = Integer.parseInt(pubSubCluster.getKey());
      Map<String, String> mappings = pubSubCluster.getValue();

      String alias = mappings.get(KAFKA_CLUSTER_MAP_KEY_NAME);
      if (alias != null) {
        tmpPubSubClusterIdToAliasMap.put(clusterId, alias);
        tmpPubSubClusterAliasToIdMap.put(alias, clusterId);
      }

      String securityProtocolString = mappings.get(KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL);

      String url = mappings.get(KAFKA_CLUSTER_MAP_KEY_URL);
      if (url != null) {
        tmpPubSubClusterIdToUrlMap.put(clusterId, url);
        tmpPubSubClusterUrlToIdMap.put(url, clusterId);
        tmpPubSubUrlResolution.put(url, url);
        if (securityProtocolString != null) {
          tmpPubSubBootstrapUrlToSecurityProtocol.put(url, SecurityProtocol.valueOf(securityProtocolString));
        }
      }
      if (basePubSubBootstrapServers.equals(url)) {
        foundBasePubSubUrlInMappingIfItIsPopulated = true;
      }

      String otherUrls = mappings.get(KAFKA_CLUSTER_MAP_KEY_OTHER_URLS);
      if (otherUrls != null) {
        String[] otherUrlsList = otherUrls.split(",");
        for (String otherUrl: otherUrlsList) {
          if (basePubSubBootstrapServers.equals(otherUrl)) {
            foundBasePubSubUrlInMappingIfItIsPopulated = true;
          }

          tmpPubSubClusterUrlToIdMap.put(otherUrl, clusterId);
          String previousMappingForSameName = tmpPubSubUrlResolution.put(otherUrl, url);
          if (previousMappingForSameName != null) {
            throw new IllegalArgumentException(
                "Alternative URLs must be unique, they cannot map to two different PubSub clusters!");
          }
        }
      }
    }
    if (!foundBasePubSubUrlInMappingIfItIsPopulated) {
      LOGGER.info(
          "basePubSubBootstrapServers ({}) not found in PubSub cluster mapping: {}",
          basePubSubBootstrapServers,
          pubSubClusterMap);
    }

    Map<String, String> tmpPubSubClusterUrlToAliasMap = new HashMap<>();
    for (Object2IntMap.Entry<String> entry: tmpPubSubClusterUrlToIdMap.object2IntEntrySet()) {
      String pubSubClusterAlias = tmpPubSubClusterIdToAliasMap.get(entry.getIntValue());
      tmpPubSubClusterUrlToAliasMap.put(entry.getKey(), pubSubClusterAlias);
    }

    String pubSubSecurityProtocolString =
        clusterProps.getString(KAFKA_SECURITY_PROTOCOL, SecurityProtocol.PLAINTEXT.name());
    if (!KafkaSSLUtils.isKafkaProtocolValid(pubSubSecurityProtocolString)) {
      throw new ConfigurationException("Invalid pubSub security protocol: " + pubSubSecurityProtocolString);
    }
    /**
     * If the {@link pubSubClusterIdToUrlMap} and {@link pubSubClusterUrlToIdMap} are equal in size, then it means
     * that {@link KAFKA_CLUSTER_MAP_KEY_OTHER_URLS} was never specified in the {@link pubSubClusterMap}, in which
     * case, the resolver needs not lookup anything, and it will always return the same as its input.
     */
    pubSubClusterUrlResolver = tmpPubSubClusterIdToUrlMap.size() == tmpPubSubClusterUrlToIdMap.size()
        ? String::toString
        : url -> tmpPubSubUrlResolution.getOrDefault(url, url);
    pubSubBootstrapServers = pubSubClusterUrlResolver.apply(basePubSubBootstrapServers);
    if (pubSubBootstrapServers == null || pubSubBootstrapServers.isEmpty()) {
      throw new ConfigurationException("pubSubBootstrapServers can't be empty");
    }

    SecurityProtocol pubSubSecurityProtocol = SecurityProtocol.forName(pubSubSecurityProtocolString);
    if (!KafkaSSLUtils.isKafkaProtocolValid(pubSubSecurityProtocolString)) {
      throw new ConfigurationException("Invalid pubSub security protocol: " + pubSubSecurityProtocolString);
    }

    return new PubSubClusterInfo(
        pubSubBootstrapServers,
        Int2ObjectMaps.unmodifiable(tmpPubSubClusterIdToUrlMap),
        Object2IntMaps.unmodifiable(tmpPubSubClusterUrlToIdMap),
        Int2ObjectMaps.unmodifiable(tmpPubSubClusterIdToAliasMap),
        Object2IntMaps.unmodifiable(tmpPubSubClusterAliasToIdMap),
        Collections.unmodifiableMap(tmpPubSubClusterUrlToAliasMap),
        pubSubClusterMap,
        Collections.unmodifiableMap(tmpPubSubUrlResolution),
        Collections.unmodifiableMap(tmpPubSubBootstrapUrlToSecurityProtocol),
        pubSubSecurityProtocol,
        pubSubClusterUrlResolver);
  }
}
