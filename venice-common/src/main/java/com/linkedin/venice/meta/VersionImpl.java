package com.linkedin.venice.meta;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.utils.AvroCompatibilityUtils;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Class defines the version of Venice store.
 */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class VersionImpl implements Version {
  @JsonIgnore
  private final String kafkaTopicName;

  // The internal data model
  private final StoreVersion storeVersion;

  /**
   * Use the constructor that specifies a pushJobId instead
   */
  @Deprecated
  public VersionImpl(String storeName, int number) {
    this(storeName , number, System.currentTimeMillis(), Version.numberBasedDummyPushId(number), 0, new PartitionerConfigImpl());
  }

  public VersionImpl(String storeName, int number, String pushJobId) {
    this(storeName, number, System.currentTimeMillis(), pushJobId, 0, new PartitionerConfigImpl());
  }

  public VersionImpl(String storeName, int number, String pushJobId, int partitionCount) {
    this(storeName, number, System.currentTimeMillis(), pushJobId, partitionCount, new PartitionerConfigImpl());
  }

  public VersionImpl(
      @JsonProperty("storeName") @com.fasterxml.jackson.annotation.JsonProperty("storeName") String storeName,
      @JsonProperty("number") @com.fasterxml.jackson.annotation.JsonProperty("number") int number,
      @JsonProperty("createdTime")  @com.fasterxml.jackson.annotation.JsonProperty("createdTime") long createdTime,
      @JsonProperty("pushJobId") @com.fasterxml.jackson.annotation.JsonProperty("pushJobId") String pushJobId,
      @JsonProperty("partitionCount") @com.fasterxml.jackson.annotation.JsonProperty("partitionCount") int partitionCount,
      @JsonProperty("partitionerConfig") @com.fasterxml.jackson.annotation.JsonProperty("partitionerConfig") PartitionerConfig partitionerConfig) {
    this.storeVersion = Store.prefillAvroRecordWithDefaultValue(new StoreVersion());
    this.storeVersion.storeName = storeName;
    this.storeVersion.number = number;
    this.storeVersion.createdTime = createdTime;
    this.storeVersion.pushJobId = pushJobId == null ? Version.numberBasedDummyPushId(number) : pushJobId; // for deserializing old Versions that didn't get an pushJobId
    this.storeVersion.partitionCount = partitionCount;
    if (partitionerConfig != null) {
      this.storeVersion.partitionerConfig = partitionerConfig.dataModel();
    }

    this.kafkaTopicName = Version.composeKafkaTopic(storeName, number);
  }

  VersionImpl(StoreVersion storeVersion) {
    this.storeVersion = storeVersion;
    this.kafkaTopicName = Version.composeKafkaTopic(getStoreName(), getNumber());
  }

  @Override
  public int getNumber() {
    return this.storeVersion.number;
  }

  @Override
  public long getCreatedTime() {
    return this.storeVersion.createdTime;
  }

  @JsonIgnore
  @Override
  public Duration getAge() {
    return Duration.ofMillis(System.currentTimeMillis() - getCreatedTime());
  }

  @JsonIgnore
  @Override
  public void setAge(Duration age) {
    this.storeVersion.createdTime = System.currentTimeMillis() - age.toMillis();
  }

  @Override
  public VersionStatus getStatus() {
    return VersionStatus.getVersionStatusFromInt(this.storeVersion.status);
  }

  @Override
  public void setStatus(VersionStatus status) {
    this.storeVersion.status = status.ordinal();
  }

  @Override
  public CompressionStrategy getCompressionStrategy() {
    return CompressionStrategy.valueOf(this.storeVersion.compressionStrategy);
  }

  @Override
  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.storeVersion.compressionStrategy = compressionStrategy.getValue();
  }

  @Override
  public boolean isLeaderFollowerModelEnabled() {
    return this.storeVersion.leaderFollowerModelEnabled;
  }

  @Override
  public boolean isNativeReplicationEnabled() {
    return this.storeVersion.nativeReplicationEnabled;
  }

  @Override
  public void setLeaderFollowerModelEnabled(boolean leaderFollowerModelEnabled) {
    this.storeVersion.leaderFollowerModelEnabled = leaderFollowerModelEnabled;
  }

  @Override
  public void setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
    this.storeVersion.nativeReplicationEnabled = nativeReplicationEnabled;
  }

  @Override
  public String getPushStreamSourceAddress() {
    return this.storeVersion.pushStreamSourceAddress.toString();
  }

  @Override
  public void setPushStreamSourceAddress(String address) {
    this.storeVersion.pushStreamSourceAddress = address;
  }

  @Override
  public boolean isBufferReplayEnabledForHybrid() {
    return this.storeVersion.bufferReplayEnabledForHybrid;
  }

  @Override
  public void setBufferReplayEnabledForHybrid(boolean bufferReplayEnabledForHybrid) {
    this.storeVersion.bufferReplayEnabledForHybrid = bufferReplayEnabledForHybrid;
  }

  @Override
  public boolean isChunkingEnabled() {
    return this.storeVersion.chunkingEnabled;
  }

  @Override
  public void setChunkingEnabled(boolean chunkingEnabled) {
    this.storeVersion.chunkingEnabled = chunkingEnabled;
  }

  @Override
  public String getStoreName() {
    return this.storeVersion.storeName.toString();
  }

  @Override
  public String getPushJobId() {
    return this.storeVersion.pushJobId.toString();
  }

  @Override
  public PushType getPushType() {
    return PushType.valueOf(this.storeVersion.pushType);
  }

  @Override
  public void setPushType(PushType pushType) {
    this.storeVersion.pushType = pushType.getValue();
  }

  @Override
  public void setPartitionCount(int partitionCount) {
    this.storeVersion.partitionCount = partitionCount;
  }

  @Override
  public int getPartitionCount() {
    return this.storeVersion.partitionCount;
  }

  @Override
  public PartitionerConfig getPartitionerConfig() {
    if (null == this.storeVersion.partitionerConfig) {
      return null;
    }
    return new PartitionerConfigImpl(this.storeVersion.partitionerConfig);
  }

  @Override
  public void setPartitionerConfig(PartitionerConfig partitionerConfig) {
    if (partitionerConfig != null) {
      this.storeVersion.partitionerConfig = partitionerConfig.dataModel();
    }
  }

  @Override
  public IncrementalPushPolicy getIncrementalPushPolicy() {
    return IncrementalPushPolicy.valueOf(this.storeVersion.incrementalPushPolicy);
  }

  @Override
  public void setIncrementalPushPolicy(IncrementalPushPolicy incrementalPushPolicy) {
    this.storeVersion.incrementalPushPolicy = incrementalPushPolicy.getValue();
  }

  @Override
  public int getReplicationFactor() {
    return this.storeVersion.replicationFactor;
  }

  @Override
  public void setReplicationFactor(int replicationFactor) {
    this.storeVersion.replicationFactor = replicationFactor;
  }

  @Override
  public int getMinActiveReplicas() {
    return this.storeVersion.replicationFactor - 1;
  }

  @Override
  public String getNativeReplicationSourceFabric() {
    return this.storeVersion.nativeReplicationSourceFabric.toString();
  }

  @Override
  public void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric) {
    this.storeVersion.nativeReplicationSourceFabric = nativeReplicationSourceFabric;
  }

  @Override
  public boolean isIncrementalPushEnabled() {
    return this.storeVersion.incrementalPushEnabled;
  }

  @Override
  public void setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    this.storeVersion.incrementalPushEnabled = incrementalPushEnabled;
  }

  @Override
  public boolean isUseVersionLevelIncrementalPushEnabled() {
    return this.storeVersion.useVersionLevelIncrementalPushEnabled;
  }

  @Override
  public void setUseVersionLevelIncrementalPushEnabled(boolean versionLevelIncrementalPushEnabled) {
    this.storeVersion.useVersionLevelIncrementalPushEnabled = versionLevelIncrementalPushEnabled;
  }

  @Override
  public HybridStoreConfig getHybridStoreConfig() {
    if (null == this.storeVersion.hybridConfig) {
      return null;
    }
    return new HybridStoreConfigImpl(this.storeVersion.hybridConfig);
  }

  @Override
  public void setHybridStoreConfig(HybridStoreConfig hybridConfig) {
    if (hybridConfig != null) {
      this.storeVersion.hybridConfig = hybridConfig.dataModel();
    }
  }

  @Override
  public boolean isUseVersionLevelHybridConfig() {
    return this.storeVersion.useVersionLevelHybridConfig;
  }

  @Override
  public void setUseVersionLevelHybridConfig(boolean versionLevelHybridConfig) {
    this.storeVersion.useVersionLevelHybridConfig = versionLevelHybridConfig;
  }

  @Override
  public StoreVersion dataModel() {
    return this.storeVersion;
  }


  @Override
  public String toString() {
    return "Version{" +
        "storeName='" + getStoreName() + '\'' +
        ", number=" + getNumber() +
        ", createdTime=" + getCreatedTime() +
        ", status=" + getStatus() +
        ", pushJobId='" + getPushJobId() + '\'' +
        ", compressionStrategy='" + getCompressionStrategy() + '\'' +
        ", leaderFollowerModelEnabled=" + isLeaderFollowerModelEnabled() +
        ", bufferReplayEnabledForHybrid=" + isBufferReplayEnabledForHybrid() +
        ", pushType=" + getPushType() +
        ", partitionCount=" + getPartitionCount() +
        ", partitionerConfig=" + getPartitionerConfig() +
        ", nativeReplicationEnabled=" + isNativeReplicationEnabled() +
        ", pushStreamSourceAddress=" + getPushStreamSourceAddress() +
        ", replicationFactor=" + getReplicationFactor() +
        ", nativeReplicationSourceFabric=" + getNativeReplicationSourceFabric() +
        ", incrementalPushEnabled=" + isIncrementalPushEnabled() +
        ", useVersionLevelIncrementalPushEnabled=" + isUseVersionLevelIncrementalPushEnabled() +
        ", hybridConfig=" + getHybridStoreConfig() +
        ", useVersionLevelHybridConfig=" + isUseVersionLevelHybridConfig() +
        '}';
  }

  @Override
  public int compareTo(Version o) {
    if(o == null) {
      throw new IllegalArgumentException("Input argument is null");
    }

    Integer num = this.storeVersion.number;
    return num.compareTo(o.getNumber());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VersionImpl version = (VersionImpl) o;
    return AvroCompatibilityUtils.compare(storeVersion, version.storeVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storeVersion);
  }

  /**
   * Clone a new version based on current data in this version.
   *
   * @return cloned version.
   */
  @JsonIgnore
  public Version cloneVersion() {
    Version clonedVersion = new VersionImpl(getStoreName(), getNumber(), getCreatedTime(), getPushJobId(), getPartitionCount(), getPartitionerConfig());
    clonedVersion.setStatus(getStatus());
    clonedVersion.setCompressionStrategy(getCompressionStrategy());
    clonedVersion.setLeaderFollowerModelEnabled(isLeaderFollowerModelEnabled());
    clonedVersion.setBufferReplayEnabledForHybrid(isBufferReplayEnabledForHybrid());
    clonedVersion.setChunkingEnabled(isChunkingEnabled());
    clonedVersion.setPushType(getPushType());
    clonedVersion.setNativeReplicationEnabled(isNativeReplicationEnabled());
    clonedVersion.setPushStreamSourceAddress(getPushStreamSourceAddress());
    clonedVersion.setIncrementalPushPolicy(getIncrementalPushPolicy());
    clonedVersion.setReplicationFactor(getReplicationFactor());
    clonedVersion.setNativeReplicationSourceFabric(getNativeReplicationSourceFabric());
    clonedVersion.setIncrementalPushEnabled(isIncrementalPushEnabled());
    clonedVersion.setUseVersionLevelIncrementalPushEnabled(isUseVersionLevelIncrementalPushEnabled());
    clonedVersion.setHybridStoreConfig(getHybridStoreConfig());
    clonedVersion.setUseVersionLevelHybridConfig(isUseVersionLevelHybridConfig());
    return clonedVersion;
  }

  /**
   * Kafka topic name is composed by store name and version.
   * <p>
   * The Json deserializer will think it should be a field called kafkaTopicName if we use "getKafkaTopicName" here. So
   * use "kafkaTopicName" directly here to avoid error when deserialize.
   *
   * @return kafka topic name.
   */
  @JsonIgnore
  public String kafkaTopicName() {
    return kafkaTopicName;
  }
}
