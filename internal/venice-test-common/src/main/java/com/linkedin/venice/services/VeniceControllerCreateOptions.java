package com.linkedin.venice.services;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.services.VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;

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
  private final int minActiveReplica;
  private final long rebalanceDelayMs;
  private final String[] clusterNames;
  private final Map<String, String> clusterToD2;
  private final VeniceControllerWrapper[] childControllers;
  private final ZkServerWrapper zkServer;
  private final KafkaBrokerWrapper kafkaBroker;
  private final Properties extraProperties;
  private final AuthorizerService authorizerService;
  private final String coloName;

  private VeniceControllerCreateOptions(Builder builder) {
    sslToKafka = builder.sslToKafka;
    d2Enabled = builder.d2Enabled;
    replicationFactor = builder.replicationFactor;
    partitionSize = builder.partitionSize;
    minActiveReplica = builder.minActiveReplica;
    rebalanceDelayMs = builder.rebalanceDelayMs;
    clusterNames = builder.clusterNames;
    clusterToD2 = builder.clusterToD2;
    childControllers = builder.childControllers;
    zkServer = builder.zkServer;
    kafkaBroker = builder.kafkaBroker;
    extraProperties = builder.extraProperties;
    authorizerService = builder.authorizerService;
    isParent = builder.childControllers != null && builder.childControllers.length != 0;
    coloName = builder.coloName;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("coloName:")
        .append(coloName)
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

  public VeniceControllerWrapper[] getChildControllers() {
    return childControllers;
  }

  public KafkaBrokerWrapper getKafkaBroker() {
    return kafkaBroker;
  }

  public Properties getExtraProperties() {
    return extraProperties;
  }

  public AuthorizerService getAuthorizerService() {
    return authorizerService;
  }

  public String getColoName() {
    return coloName;
  }

  public static class Builder {
    private final String[] clusterNames;
    private final ZkServerWrapper zkServer;
    private final KafkaBrokerWrapper kafkaBroker;
    private boolean sslToKafka = false;
    private boolean d2Enabled = false;
    private boolean isMinActiveReplicaSet = false;
    private int replicationFactor = VeniceClusterWrapperConstants.DEFAULT_REPLICATION_FACTOR;
    private int partitionSize = VeniceClusterWrapperConstants.DEFAULT_PARTITION_SIZE_BYTES;
    private int minActiveReplica;
    private long rebalanceDelayMs = VeniceClusterWrapperConstants.DEFAULT_DELAYED_TO_REBALANCE_MS;
    private Map<String, String> clusterToD2 = null;
    private VeniceControllerWrapper[] childControllers = null;
    private Properties extraProperties = new Properties();
    private AuthorizerService authorizerService;
    private String coloName = "";

    public Builder(String[] clusterNames, ZkServerWrapper zkServer, KafkaBrokerWrapper kafkaBroker) {
      this.clusterNames = Objects.requireNonNull(clusterNames, "clusterNames cannot be null when creating controller");
      this.zkServer = Objects.requireNonNull(zkServer, "ZkServerWrapper cannot be null when creating controller");
      this.kafkaBroker =
          Objects.requireNonNull(kafkaBroker, "KafkaBrokerWrapper cannot be null when creating controller");
    }

    public Builder(String clusterName, ZkServerWrapper zkServer, KafkaBrokerWrapper kafkaBroker) {
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

    public Builder coloName(String coloName) {
      this.coloName = coloName;
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
