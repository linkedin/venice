package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_DELAYED_TO_REBALANCE_MS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_CONTROLLERS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_ROUTERS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_SERVERS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARTITION_SIZE_BYTES;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_REPLICATION_FACTOR;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_SSL_TO_KAFKA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_SSL_TO_STORAGE_NODES;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.STANDALONE_REGION_NAME;

import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class VeniceClusterCreateOptions {
  private final String clusterName;
  private final String regionName;
  private final Map<String, String> clusterToD2;
  private final Map<String, String> clusterToServerD2;
  private final int numberOfControllers;
  private final int numberOfServers;
  private final int numberOfRouters;
  private final int replicationFactor;
  private final int partitionSize;
  private final int numberOfPartitions;
  private final int maxNumberOfPartitions;
  private final int minActiveReplica;
  private final long rebalanceDelayMs;
  private final boolean standalone;
  private final boolean enableAllowlist;
  private final boolean enableAutoJoinAllowlist;
  private final boolean sslToStorageNodes;
  private final boolean sslToKafka;
  private final boolean forkServer;
  private final boolean enableGrpc;
  private final Properties extraProperties;
  private final Map<String, Map<String, String>> kafkaClusterMap;
  private final ZkServerWrapper zkServerWrapper;
  private final PubSubBrokerWrapper pubSubBrokerWrapper;

  private VeniceClusterCreateOptions(Builder builder) {
    this.clusterName = builder.clusterName;
    this.regionName = builder.regionName;
    this.clusterToD2 = builder.clusterToD2;
    this.clusterToServerD2 = builder.clusterToServerD2;
    this.numberOfControllers = builder.numberOfControllers;
    this.numberOfServers = builder.numberOfServers;
    this.numberOfRouters = builder.numberOfRouters;
    this.replicationFactor = builder.replicationFactor;
    this.partitionSize = builder.partitionSize;
    this.numberOfPartitions = builder.numberOfPartitions;
    this.maxNumberOfPartitions = builder.maxNumberOfPartitions;
    this.minActiveReplica = builder.minActiveReplica;
    this.rebalanceDelayMs = builder.rebalanceDelayMs;
    this.standalone = builder.standalone;
    this.enableAllowlist = builder.enableAllowlist;
    this.enableAutoJoinAllowlist = builder.enableAutoJoinAllowlist;
    this.sslToStorageNodes = builder.sslToStorageNodes;
    this.sslToKafka = builder.sslToKafka;
    this.forkServer = builder.forkServer;
    this.enableGrpc = builder.enableGrpc;
    this.extraProperties = builder.extraProperties;
    this.kafkaClusterMap = builder.kafkaClusterMap;
    this.zkServerWrapper = builder.zkServerWrapper;
    this.pubSubBrokerWrapper = builder.pubSubBrokerWrapper;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getRegionName() {
    return regionName;
  }

  public Map<String, String> getClusterToD2() {
    return clusterToD2;
  }

  public Map<String, String> getClusterToServerD2() {
    return clusterToServerD2;
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

  public int getNumberOfPartitions() {
    return numberOfPartitions;
  }

  public int getMaxNumberOfPartitions() {
    return maxNumberOfPartitions;
  }

  public int getMinActiveReplica() {
    return minActiveReplica;
  }

  public long getRebalanceDelayMs() {
    return rebalanceDelayMs;
  }

  public boolean isStandalone() {
    return standalone;
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

  public boolean isForkServer() {
    return forkServer;
  }

  public boolean isGrpcEnabled() {
    return enableGrpc;
  }

  public Properties getExtraProperties() {
    return extraProperties;
  }

  public Map<String, Map<String, String>> getKafkaClusterMap() {
    return kafkaClusterMap;
  }

  public ZkServerWrapper getZkServerWrapper() {
    return zkServerWrapper;
  }

  public PubSubBrokerWrapper getKafkaBrokerWrapper() {
    return pubSubBrokerWrapper;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("VeniceClusterCreateOptions - ")
        .append("cluster:")
        .append(clusterName)
        .append(", ")
        .append("standalone:")
        .append(standalone)
        .append(", ")
        .append("regionName:")
        .append(regionName)
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
        .append("numberOfPartitions:")
        .append(numberOfPartitions)
        .append(", ")
        .append("maxNumberOfPartitions:")
        .append(maxNumberOfPartitions)
        .append(", ")
        .append("minActiveReplica:")
        .append(minActiveReplica)
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
        .append("enableGrpc:")
        .append(enableGrpc)
        .append(", ")
        .append("extraProperties:")
        .append(extraProperties)
        .append(", ")
        .append("clusterToD2:")
        .append(clusterToD2)
        .append(",")
        .append("clusterToServerD2:")
        .append(clusterToServerD2)
        .append(", ")
        .append("zk:")
        .append(zkServerWrapper == null ? "null" : zkServerWrapper.getAddress())
        .append(", ")
        .append("kafka:")
        .append(pubSubBrokerWrapper == null ? "null" : pubSubBrokerWrapper.getAddress())
        .append(", ")
        .append("kafkaClusterMap:")
        .append(kafkaClusterMap)
        .toString();
  }

  public static class Builder {
    private String clusterName;
    private String regionName;
    private Map<String, String> clusterToD2 = null;
    private Map<String, String> clusterToServerD2 = null;
    private int numberOfControllers = DEFAULT_NUMBER_OF_CONTROLLERS;
    private int numberOfServers = DEFAULT_NUMBER_OF_SERVERS;
    private int numberOfRouters = DEFAULT_NUMBER_OF_ROUTERS;
    private int replicationFactor = DEFAULT_REPLICATION_FACTOR;
    private int partitionSize = DEFAULT_PARTITION_SIZE_BYTES;
    private int numberOfPartitions = DEFAULT_NUMBER_OF_PARTITIONS;
    private int maxNumberOfPartitions = DEFAULT_MAX_NUMBER_OF_PARTITIONS;
    private int minActiveReplica;
    private long rebalanceDelayMs = DEFAULT_DELAYED_TO_REBALANCE_MS;
    private boolean standalone = true; // set to false for multi-cluster
    private boolean enableAllowlist;
    private boolean enableAutoJoinAllowlist;
    private boolean sslToStorageNodes = DEFAULT_SSL_TO_STORAGE_NODES;
    private boolean sslToKafka = DEFAULT_SSL_TO_KAFKA;
    private boolean forkServer;
    private boolean isMinActiveReplicaSet = false;
    private boolean enableGrpc = false;
    private Properties extraProperties;
    private Map<String, Map<String, String>> kafkaClusterMap;
    private ZkServerWrapper zkServerWrapper;
    private PubSubBrokerWrapper pubSubBrokerWrapper;

    public Builder clusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder regionName(String regionName) {
      this.regionName = regionName;
      return this;
    }

    public Builder clusterToD2(Map<String, String> clusterToD2) {
      this.clusterToD2 = clusterToD2;
      return this;
    }

    public Builder clusterToServerD2(Map<String, String> clusterToServerD2) {
      this.clusterToServerD2 = clusterToServerD2;
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

    public Builder numberOfPartitions(int numberOfPartitions) {
      this.numberOfPartitions = numberOfPartitions;
      return this;
    }

    public Builder maxNumberOfPartitions(int maxNumberOfPartitions) {
      this.maxNumberOfPartitions = maxNumberOfPartitions;
      return this;
    }

    public Builder minActiveReplica(int minActiveReplica) {
      this.minActiveReplica = minActiveReplica;
      this.isMinActiveReplicaSet = true;
      return this;
    }

    public Builder enableGrpc(boolean enableGrpc) {
      this.enableGrpc = enableGrpc;
      return this;
    }

    public Builder rebalanceDelayMs(long rebalanceDelayMs) {
      this.rebalanceDelayMs = rebalanceDelayMs;
      return this;
    }

    public Builder standalone(boolean standalone) {
      this.standalone = standalone;
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

    public Builder forkServer(boolean forkServer) {
      this.forkServer = forkServer;
      return this;
    }

    public Builder extraProperties(Properties extraProperties) {
      this.extraProperties = extraProperties;
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

    public Builder kafkaBrokerWrapper(PubSubBrokerWrapper pubSubBrokerWrapper) {
      this.pubSubBrokerWrapper = pubSubBrokerWrapper;
      return this;
    }

    private void verifyAndAddDefaults() {
      if (clusterName == null) {
        clusterName = Utils.getUniqueString("venice-cluster");
      }
      if (regionName == null || regionName.isEmpty()) {
        regionName = STANDALONE_REGION_NAME;
      }
      if (!isMinActiveReplicaSet) {
        minActiveReplica = replicationFactor - 1;
      }
      if (extraProperties == null) {
        extraProperties = new Properties();
      }
      if (kafkaClusterMap == null) {
        kafkaClusterMap = Collections.emptyMap();
      }
    }

    public VeniceClusterCreateOptions build() {
      verifyAndAddDefaults();
      return new VeniceClusterCreateOptions(this);
    }
  }
}
