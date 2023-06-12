package com.linkedin.venice.integration.utils;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_DELAYED_TO_REBALANCE_MS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARTITION_SIZE_BYTES;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_REPLICATION_FACTOR;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;

import com.linkedin.venice.authorization.AuthorizerService;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;


public class VeniceControllerCreateOptions {
  private final boolean isParent;
  private final boolean sslToKafka;
  private final boolean d2Enabled;
  private final int replicationFactor;
  private final int partitionSize;
  private final int numberOfPartitions;
  private final int maxNumberOfPartitions;
  private final int minActiveReplica;
  private final long rebalanceDelayMs;
  private final String[] clusterNames;
  private final Map<String, String> clusterToD2;
  private final Map<String, String> clusterToServerD2;
  private final VeniceControllerWrapper[] childControllers;
  private final ZkServerWrapper zkServer;
  private final PubSubBrokerWrapper kafkaBroker;
  private final Properties extraProperties;
  private final AuthorizerService authorizerService;
  private final String regionName;

  private VeniceControllerCreateOptions(Builder builder) {
    sslToKafka = builder.sslToKafka;
    d2Enabled = builder.d2Enabled;
    replicationFactor = builder.replicationFactor;
    partitionSize = builder.partitionSize;
    numberOfPartitions = builder.numberOfPartitions;
    maxNumberOfPartitions = builder.maxNumberOfPartitions;
    minActiveReplica = builder.minActiveReplica;
    rebalanceDelayMs = builder.rebalanceDelayMs;
    clusterNames = builder.clusterNames;
    clusterToD2 = builder.clusterToD2;
    clusterToServerD2 = builder.clusterToServerD2;
    childControllers = builder.childControllers;
    zkServer = builder.zkServer;
    kafkaBroker = builder.kafkaBroker;
    extraProperties = builder.extraProperties;
    authorizerService = builder.authorizerService;
    isParent = builder.childControllers != null && builder.childControllers.length != 0;
    regionName = builder.regionName;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("regionName:")
        .append(regionName)
        .append(", ")
        .append("isParent:")
        .append(isParent)
        .append(", ")
        .append("sslToKafka:")
        .append(sslToKafka)
        .append(", ")
        .append("replicationFactor:")
        .append(replicationFactor)
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
        .append("rebalanceDelayMs:")
        .append(rebalanceDelayMs)
        .append(", ")
        .append("clusterNames:")
        .append(Arrays.toString(clusterNames))
        .append(", ")
        .append("zkAddress:")
        .append(zkServer.getAddress())
        .append(", ")
        .append("kafkaBroker:")
        .append(kafkaBroker == null ? "null" : kafkaBroker.getAddress())
        .append(", ")
        .append("d2Enabled:")
        .append(d2Enabled)
        .append(", ")
        .append("clusterToD2:")
        .append(clusterToD2)
        .append(",")
        .append("clusterToServerD2:")
        .append(clusterToServerD2)
        .append(", ")
        .append("extraProperties:")
        .append(extraProperties)
        .append(", ")
        .append("childControllers:")
        .append(getAddressesOfChildControllers())
        .toString();
  }

  private String getAddressesOfChildControllers() {
    if (childControllers == null) {
      return "null";
    }
    return Arrays.stream(childControllers)
        .map(VeniceControllerWrapper::getControllerUrl)
        .collect(Collectors.toList())
        .toString();
  }

  public boolean isParent() {
    return isParent;
  }

  public boolean isSslToKafka() {
    return sslToKafka;
  }

  public boolean isD2Enabled() {
    return d2Enabled;
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

  public String[] getClusterNames() {
    return clusterNames;
  }

  public String getZkAddress() {
    return zkServer.getAddress();
  }

  public Map<String, String> getClusterToD2() {
    return clusterToD2;
  }

  public Map<String, String> getClusterToServerD2() {
    return clusterToServerD2;
  }

  public VeniceControllerWrapper[] getChildControllers() {
    return childControllers;
  }

  public PubSubBrokerWrapper getKafkaBroker() {
    return kafkaBroker;
  }

  public Properties getExtraProperties() {
    return extraProperties;
  }

  public AuthorizerService getAuthorizerService() {
    return authorizerService;
  }

  public String getRegionName() {
    return regionName;
  }

  public static class Builder {
    private final String[] clusterNames;
    private final ZkServerWrapper zkServer;
    private final PubSubBrokerWrapper kafkaBroker;
    private boolean sslToKafka = false;
    private boolean d2Enabled = false;
    private boolean isMinActiveReplicaSet = false;
    private int replicationFactor = DEFAULT_REPLICATION_FACTOR;
    private int partitionSize = DEFAULT_PARTITION_SIZE_BYTES;
    private int numberOfPartitions = DEFAULT_NUMBER_OF_PARTITIONS;
    private int maxNumberOfPartitions = DEFAULT_MAX_NUMBER_OF_PARTITIONS;
    private int minActiveReplica;
    private long rebalanceDelayMs = DEFAULT_DELAYED_TO_REBALANCE_MS;
    private Map<String, String> clusterToD2 = null;
    private Map<String, String> clusterToServerD2 = null;
    private VeniceControllerWrapper[] childControllers = null;
    private Properties extraProperties = new Properties();
    private AuthorizerService authorizerService;
    private String regionName = "";

    public Builder(String[] clusterNames, ZkServerWrapper zkServer, PubSubBrokerWrapper kafkaBroker) {
      this.clusterNames = Objects.requireNonNull(clusterNames, "clusterNames cannot be null when creating controller");
      this.zkServer = Objects.requireNonNull(zkServer, "ZkServerWrapper cannot be null when creating controller");
      this.kafkaBroker =
          Objects.requireNonNull(kafkaBroker, "KafkaBrokerWrapper cannot be null when creating controller");
    }

    public Builder(String clusterName, ZkServerWrapper zkServer, PubSubBrokerWrapper kafkaBroker) {
      this(new String[] { clusterName }, zkServer, kafkaBroker);
    }

    public Builder sslToKafka(boolean sslToKafka) {
      this.sslToKafka = sslToKafka;
      return this;
    }

    public Builder d2Enabled(boolean d2Enabled) {
      this.d2Enabled = d2Enabled;
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

    public Builder rebalanceDelayMs(long rebalanceDelayMs) {
      this.rebalanceDelayMs = rebalanceDelayMs;
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

    public Builder childControllers(VeniceControllerWrapper[] childControllers) {
      this.childControllers = childControllers;
      return this;
    }

    public Builder extraProperties(Properties extraProperties) {
      this.extraProperties = extraProperties;
      return this;
    }

    public Builder authorizerService(AuthorizerService authorizerService) {
      this.authorizerService = authorizerService;
      return this;
    }

    public Builder regionName(String regionName) {
      this.regionName = regionName;
      return this;
    }

    private void verifyAndAddParentControllerSpecificDefaults() {
      if (!isMinActiveReplicaSet) {
        minActiveReplica = replicationFactor > 1 ? replicationFactor - 1 : replicationFactor;
      }
      extraProperties.setProperty(LOCAL_REGION_NAME, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
      if (!extraProperties.containsKey(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE)) {
        extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "true");
      }
      if (!extraProperties.containsKey(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE)) {
        extraProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "true");
      }
      d2Enabled = clusterToD2 != null;
    }

    private void verifyAndAddChildControllerSpecificDefaults() {
      if (!isMinActiveReplicaSet) {
        minActiveReplica = replicationFactor;
      }
    }

    private void addDefaults() {
      if (extraProperties == null) {
        extraProperties = new Properties();
      }
      if (childControllers != null && childControllers.length != 0) {
        verifyAndAddParentControllerSpecificDefaults();
      } else {
        verifyAndAddChildControllerSpecificDefaults();
      }
    }

    public VeniceControllerCreateOptions build() {
      addDefaults();
      return new VeniceControllerCreateOptions(this);
    }
  }
}
