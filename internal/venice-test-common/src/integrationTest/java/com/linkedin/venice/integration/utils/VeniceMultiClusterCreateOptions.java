package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_DELAYED_TO_REBALANCE_MS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_CONTROLLERS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_ROUTERS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_SERVERS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARTITION_SIZE_BYTES;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_REPLICATION_FACTOR;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_SSL_TO_KAFKA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_SSL_TO_STORAGE_NODES;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.STANDALONE_REGION_NAME;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.acl.DynamicAccessController;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class VeniceMultiClusterCreateOptions {
  private final String regionName;
  private final boolean multiRegion;
  private final int numberOfClusters;
  private final int numberOfControllers;
  private final int numberOfServers;
  private final int numberOfRouters;
  private final int replicationFactor;
  private final int partitionSize;
  private final long rebalanceDelayMs;
  private final boolean enableAllowlist;
  private final boolean enableAutoJoinAllowlist;
  private final boolean sslToStorageNodes;
  private final boolean sslToKafka;
  private final boolean randomizeClusterName;
  private final boolean forkServer;
  private final Map<String, Map<String, String>> kafkaClusterMap;
  private final ZkServerWrapper zkServerWrapper;
  private final String veniceZkBasePath;
  private final PubSubBrokerWrapper pubSubBrokerWrapper;
  private final Properties childControllerProperties;
  private final Properties extraProperties;
  private final DynamicAccessController accessController;
  private final Map<String, D2Client> d2Clients;

  public String getRegionName() {
    return regionName;
  }

  public int getNumberOfClusters() {
    return numberOfClusters;
  }

  public int getNumberOfControllers() {
    return numberOfControllers;
  }

  public int getNumberOfServers() {
    return numberOfServers;
  }

  public int getNumberOfRouters() {
    return numberOfRouters;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public int getPartitionSize() {
    return partitionSize;
  }

  public long getRebalanceDelayMs() {
    return rebalanceDelayMs;
  }

  public boolean isEnableAllowlist() {
    return enableAllowlist;
  }

  public boolean isEnableAutoJoinAllowlist() {
    return enableAutoJoinAllowlist;
  }

  public boolean isSslToStorageNodes() {
    return sslToStorageNodes;
  }

  public boolean isSslToKafka() {
    return sslToKafka;
  }

  public boolean isRandomizeClusterName() {
    return randomizeClusterName;
  }

  public boolean isMultiRegion() {
    return multiRegion;
  }

  public boolean isForkServer() {
    return forkServer;
  }

  public Map<String, Map<String, String>> getKafkaClusterMap() {
    return kafkaClusterMap;
  }

  public ZkServerWrapper getZkServerWrapper() {
    return zkServerWrapper;
  }

  public String getVeniceZkBasePath() {
    return veniceZkBasePath;
  }

  public PubSubBrokerWrapper getKafkaBrokerWrapper() {
    return pubSubBrokerWrapper;
  }

  public Properties getChildControllerProperties() {
    return childControllerProperties;
  }

  public Properties getExtraProperties() {
    return extraProperties;
  }

  public DynamicAccessController getAccessController() {
    return accessController;
  }

  public Map<String, D2Client> getD2Clients() {
    return d2Clients;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("VeniceMultiClusterCreateOptions - ")
        .append("regionName:")
        .append(regionName)
        .append(", ")
        .append("multiRegion:")
        .append(multiRegion)
        .append(", ")
        .append("clusters:")
        .append(numberOfClusters)
        .append(", ")
        .append("controllers:")
        .append(numberOfControllers)
        .append(", ")
        .append("servers:")
        .append(numberOfServers)
        .append(", ")
        .append("routers:")
        .append(numberOfRouters)
        .append(", ")
        .append("replicationFactor:")
        .append(replicationFactor)
        .append(", ")
        .append("rebalanceDelayMs:")
        .append(rebalanceDelayMs)
        .append(", ")
        .append("partitionSize:")
        .append(partitionSize)
        .append(", ")
        .append("enableAllowlist:")
        .append(enableAllowlist)
        .append(", ")
        .append("enableAutoJoinAllowlist:")
        .append(enableAutoJoinAllowlist)
        .append(", ")
        .append("sslToStorageNodes:")
        .append(sslToStorageNodes)
        .append(", ")
        .append("sslToKafka:")
        .append(sslToKafka)
        .append(", ")
        .append("forkServer:")
        .append(forkServer)
        .append(", ")
        .append("randomizeClusterName:")
        .append(randomizeClusterName)
        .append(", ")
        .append("childControllerProperties:")
        .append(childControllerProperties)
        .append(", ")
        .append("veniceProperties:")
        .append(extraProperties)
        .append(", ")
        .append("zk:")
        .append(zkServerWrapper == null ? "null" : zkServerWrapper.getAddress())
        .append(", ")
        .append("veniceZkBasePath:")
        .append(veniceZkBasePath)
        .append(", ")
        .append("kafka:")
        .append(pubSubBrokerWrapper == null ? "null" : pubSubBrokerWrapper.getAddress())
        .append(", ")
        .append("kafkaClusterMap:")
        .append(kafkaClusterMap)
        .append("d2Clients:")
        .append(d2Clients)
        .toString();
  }

  private VeniceMultiClusterCreateOptions(Builder builder) {
    regionName = builder.regionName;
    multiRegion = builder.multiRegion;
    numberOfClusters = builder.numberOfClusters;
    numberOfControllers = builder.numberOfControllers;
    numberOfServers = builder.numberOfServers;
    numberOfRouters = builder.numberOfRouters;
    replicationFactor = builder.replicationFactor;
    partitionSize = builder.partitionSize;
    enableAllowlist = builder.enableAllowlist;
    enableAutoJoinAllowlist = builder.enableAutoJoinAllowlist;
    rebalanceDelayMs = builder.rebalanceDelayMs;
    sslToStorageNodes = builder.sslToStorageNodes;
    sslToKafka = builder.sslToKafka;
    randomizeClusterName = builder.randomizeClusterName;
    zkServerWrapper = builder.zkServerWrapper;
    veniceZkBasePath = builder.veniceZkBasePath;
    pubSubBrokerWrapper = builder.pubSubBrokerWrapper;
    childControllerProperties = builder.childControllerProperties;
    extraProperties = builder.extraProperties;
    forkServer = builder.forkServer;
    kafkaClusterMap = builder.kafkaClusterMap;
    accessController = builder.accessController;
    d2Clients = builder.d2Clients;
  }

  public static class Builder {
    private String regionName;
    private boolean multiRegion = false;
    private int numberOfClusters;
    private int numberOfControllers = DEFAULT_NUMBER_OF_CONTROLLERS;
    private int numberOfServers = DEFAULT_NUMBER_OF_SERVERS;
    private int numberOfRouters = DEFAULT_NUMBER_OF_ROUTERS;
    private int replicationFactor = DEFAULT_REPLICATION_FACTOR;
    private int partitionSize = DEFAULT_PARTITION_SIZE_BYTES;
    private long rebalanceDelayMs = DEFAULT_DELAYED_TO_REBALANCE_MS;
    private boolean enableAllowlist = false;
    private boolean enableAutoJoinAllowlist = false;
    private boolean sslToStorageNodes = DEFAULT_SSL_TO_STORAGE_NODES;
    private boolean sslToKafka = DEFAULT_SSL_TO_KAFKA;
    private boolean randomizeClusterName = true;
    private boolean forkServer = false;
    private Map<String, Map<String, String>> kafkaClusterMap;
    private ZkServerWrapper zkServerWrapper;
    private String veniceZkBasePath = "/";
    private PubSubBrokerWrapper pubSubBrokerWrapper;
    private Properties childControllerProperties;
    private Properties extraProperties;
    private DynamicAccessController accessController;
    private Map<String, D2Client> d2Clients;

    public Builder numberOfClusters(int numberOfClusters) {
      this.numberOfClusters = numberOfClusters;
      return this;
    }

    public Builder regionName(String regionName) {
      this.regionName = regionName;
      return this;
    }

    public Builder numberOfControllers(int numberOfControllers) {
      this.numberOfControllers = numberOfControllers;
      return this;
    }

    public Builder numberOfServers(int numberOfServers) {
      this.numberOfServers = numberOfServers;
      return this;
    }

    public Builder numberOfRouters(int numberOfRouters) {
      this.numberOfRouters = numberOfRouters;
      return this;
    }

    public Builder replicationFactor(int replicationFactor) {
      this.replicationFactor = replicationFactor;
      return this;
    }

    public Builder partitionSize(int partitionSize) {
      this.partitionSize = partitionSize;
      return this;
    }

    public Builder rebalanceDelayMs(long rebalanceDelayMs) {
      this.rebalanceDelayMs = rebalanceDelayMs;
      return this;
    }

    public Builder enableAllowlist(boolean enableAllowlist) {
      this.enableAllowlist = enableAllowlist;
      return this;
    }

    public Builder enableAutoJoinAllowlist(boolean enableAutoJoinAllowlist) {
      this.enableAutoJoinAllowlist = enableAutoJoinAllowlist;
      return this;
    }

    public Builder sslToStorageNodes(boolean sslToStorageNodes) {
      this.sslToStorageNodes = sslToStorageNodes;
      return this;
    }

    public Builder sslToKafka(boolean sslToKafka) {
      this.sslToKafka = sslToKafka;
      return this;
    }

    public Builder randomizeClusterName(boolean randomizeClusterName) {
      this.randomizeClusterName = randomizeClusterName;
      return this;
    }

    public Builder multiRegion(boolean multiRegion) {
      this.multiRegion = multiRegion;
      return this;
    }

    public Builder forkServer(boolean forkServer) {
      this.forkServer = forkServer;
      return this;
    }

    public Builder kafkaClusterMap(Map<String, Map<String, String>> kafkaClusterMap) {
      this.kafkaClusterMap = kafkaClusterMap;
      return this;
    }

    public Builder zkServerWrapper(ZkServerWrapper zkServerWrapper) {
      this.zkServerWrapper = zkServerWrapper;
      return this;
    }

    public Builder veniceZkBasePath(String veniceZkBasePath) {
      if (veniceZkBasePath == null || !veniceZkBasePath.startsWith("/")) {
        throw new IllegalArgumentException("Venice Zk base path must start with /");
      }

      this.veniceZkBasePath = veniceZkBasePath;
      return this;
    }

    public Builder kafkaBrokerWrapper(PubSubBrokerWrapper pubSubBrokerWrapper) {
      this.pubSubBrokerWrapper = pubSubBrokerWrapper;
      return this;
    }

    public Builder childControllerProperties(Properties childControllerProperties) {
      this.childControllerProperties = childControllerProperties;
      return this;
    }

    public Builder extraProperties(Properties extraProperties) {
      this.extraProperties = extraProperties;
      return this;
    }

    public Builder accessController(DynamicAccessController accessController) {
      this.accessController = accessController;
      return this;
    }

    public Builder d2Clients(Map<String, D2Client> d2Clients) {
      this.d2Clients = d2Clients;
      return this;
    }

    private void addDefaults() {
      if (numberOfClusters == 0) {
        numberOfClusters = 1;
      }
      if (childControllerProperties == null) {
        childControllerProperties = new Properties();
      }
      if (extraProperties == null) {
        extraProperties = new Properties();
      }
      if (kafkaClusterMap == null) {
        kafkaClusterMap = Collections.emptyMap();
      }
      if (regionName == null) {
        regionName = STANDALONE_REGION_NAME;
      }
    }

    public VeniceMultiClusterCreateOptions build() {
      addDefaults();
      return new VeniceMultiClusterCreateOptions(this);
    }
  }
}
