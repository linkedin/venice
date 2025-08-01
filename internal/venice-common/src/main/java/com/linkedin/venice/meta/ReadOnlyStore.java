package com.linkedin.venice.meta;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.StoreVersionNotFoundException;
import com.linkedin.venice.systemstore.schemas.DataRecoveryConfig;
import com.linkedin.venice.systemstore.schemas.StoreETLConfig;
import com.linkedin.venice.systemstore.schemas.StoreHybridConfig;
import com.linkedin.venice.systemstore.schemas.StorePartitionerConfig;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import com.linkedin.venice.systemstore.schemas.SystemStoreProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A read-only wrapper of {@link Store}, and all the modification to delegated instance
 * will throw {@link UnsupportedOperationException}.
 */
public class ReadOnlyStore implements Store {
  /**
   * A read-only wrapper of {@link PartitionerConfig}.
   */
  private static class ReadOnlyPartitionerConfig implements PartitionerConfig {
    private final PartitionerConfig delegate;

    private ReadOnlyPartitionerConfig(PartitionerConfig delegate) {
      this.delegate = delegate;
    }

    @Override
    public String getPartitionerClass() {
      return this.delegate.getPartitionerClass();
    }

    @Override
    public Map<String, String> getPartitionerParams() {
      return Collections.unmodifiableMap(this.delegate.getPartitionerParams());
    }

    @Override
    public int getAmplificationFactor() {
      return this.delegate.getAmplificationFactor();
    }

    @Override
    public void setAmplificationFactor(int amplificationFactor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setPartitionerClass(String partitionerClass) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setPartitionerParams(Map<String, String> partitionerParams) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PartitionerConfig clone() {
      return this.delegate.clone();
    }

    @Override
    public StorePartitionerConfig dataModel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      return this.delegate.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReadOnlyPartitionerConfig config = (ReadOnlyPartitionerConfig) o;
      return this.delegate.equals(config.delegate);
    }
  }

  /**
   * A read-only wrapper of {@link HybridStoreConfig}
   */
  private static class ReadOnlyHybridStoreConfig implements HybridStoreConfig {
    private final HybridStoreConfig delegate;

    private ReadOnlyHybridStoreConfig(HybridStoreConfig delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getRewindTimeInSeconds() {
      return this.delegate.getRewindTimeInSeconds();
    }

    @Override
    public long getOffsetLagThresholdToGoOnline() {
      return this.delegate.getOffsetLagThresholdToGoOnline();
    }

    @Override
    public void setRewindTimeInSeconds(long rewindTimeInSeconds) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setOffsetLagThresholdToGoOnline(long offsetLagThresholdToGoOnline) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getProducerTimestampLagThresholdToGoOnlineInSeconds() {
      return this.delegate.getProducerTimestampLagThresholdToGoOnlineInSeconds();
    }

    @Override
    public DataReplicationPolicy getDataReplicationPolicy() {
      return this.delegate.getDataReplicationPolicy();
    }

    @Override
    public void setDataReplicationPolicy(DataReplicationPolicy dataReplicationPolicy) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BufferReplayPolicy getBufferReplayPolicy() {
      return this.delegate.getBufferReplayPolicy();
    }

    @Override
    public String getRealTimeTopicName() {
      return this.delegate.getRealTimeTopicName();
    }

    @Override
    public void setRealTimeTopicName(String realTimeTopicName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public HybridStoreConfig clone() {
      return this.delegate.clone();
    }

    @Override
    public StoreHybridConfig dataModel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      return this.delegate.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReadOnlyHybridStoreConfig config = (ReadOnlyHybridStoreConfig) o;
      return this.delegate.equals(config.delegate);
    }
  }

  /**
   * A read-only wrapper of {@link ETLStoreConfig}
   */
  private static class ReadOnlyETLStoreConfig implements ETLStoreConfig {
    private final ETLStoreConfig delegate;

    private ReadOnlyETLStoreConfig(ETLStoreConfig delegate) {
      this.delegate = delegate;
    }

    @Override
    public String getEtledUserProxyAccount() {
      return this.delegate.getEtledUserProxyAccount();
    }

    @Override
    public void setEtledUserProxyAccount(String etledUserProxyAccount) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRegularVersionETLEnabled() {
      return this.delegate.isRegularVersionETLEnabled();
    }

    @Override
    public void setRegularVersionETLEnabled(boolean regularVersionETLEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFutureVersionETLEnabled() {
      return this.delegate.isFutureVersionETLEnabled();
    }

    @Override
    public void setFutureVersionETLEnabled(boolean futureVersionETLEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ETLStoreConfig clone() {
      return this.delegate.clone();
    }

    @Override
    public StoreETLConfig dataModel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      return this.delegate.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReadOnlyETLStoreConfig config = (ReadOnlyETLStoreConfig) o;
      return this.delegate.equals(config.delegate);
    }
  }

  /**
   * A read-only wrapper of {@link DataRecoveryVersionConfig}
   */
  private static class ReadOnlyDataRecoveryVersionConfig implements DataRecoveryVersionConfig {
    private final DataRecoveryVersionConfig delegate;

    private ReadOnlyDataRecoveryVersionConfig(DataRecoveryVersionConfig delegate) {
      this.delegate = delegate;
    }

    @Override
    public DataRecoveryConfig dataModel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getDataRecoverySourceFabric() {
      return delegate.getDataRecoverySourceFabric();
    }

    @Override
    public void setDataRecoverySourceFabric(String dataRecoverySourceFabric) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDataRecoveryComplete() {
      return delegate.isDataRecoveryComplete();
    }

    @Override
    public void setDataRecoveryComplete(boolean dataRecoveryComplete) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDataRecoverySourceVersionNumber() {
      return delegate.getDataRecoverySourceVersionNumber();
    }

    @Override
    public void setDataRecoverySourceVersionNumber(int dataRecoverySourceVersionNumber) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DataRecoveryVersionConfig clone() {
      return this.delegate.clone();
    }
  }

  /**
   * A read-only wrapper of {@link Version}
   */
  public static class ReadOnlyVersion implements Version {
    protected final Version delegate;

    public ReadOnlyVersion(Version delegate) {
      this.delegate = delegate;
    }

    @Override
    public int getNumber() {
      return this.delegate.getNumber();
    }

    @Override
    public void setNumber(int number) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getCreatedTime() {
      return this.delegate.getCreatedTime();
    }

    @Override
    public Duration getAge() {
      return this.delegate.getAge();
    }

    @Override
    public void setAge(Duration age) {
      throw new UnsupportedOperationException();
    }

    @Override
    public VersionStatus getStatus() {
      return this.delegate.getStatus();
    }

    @Override
    public void setStatus(VersionStatus status) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompressionStrategy getCompressionStrategy() {
      return this.delegate.getCompressionStrategy();
    }

    @Override
    public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNativeReplicationEnabled() {
      return this.delegate.isNativeReplicationEnabled();
    }

    @Override
    public void setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getPushStreamSourceAddress() {
      return this.delegate.getPushStreamSourceAddress();
    }

    @Override
    public void setPushStreamSourceAddress(String address) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setBufferReplayEnabledForHybrid(boolean bufferReplayEnabledForHybrid) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isChunkingEnabled() {
      return this.delegate.isChunkingEnabled();
    }

    @Override
    public void setChunkingEnabled(boolean chunkingEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRmdChunkingEnabled() {
      return this.delegate.isRmdChunkingEnabled();
    }

    @Override
    public void setRmdChunkingEnabled(boolean rmdChunkingEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getStoreName() {
      return this.delegate.getStoreName();
    }

    @Override
    public String getPushJobId() {
      return this.delegate.getPushJobId();
    }

    @Override
    public void setPushJobId(String pushJobId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PushType getPushType() {
      return this.delegate.getPushType();
    }

    @Override
    public void setPushType(PushType pushType) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setPartitionCount(int partitionCount) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getPartitionCount() {
      return this.delegate.getPartitionCount();
    }

    @Override
    public PartitionerConfig getPartitionerConfig() {
      PartitionerConfig config = this.delegate.getPartitionerConfig();
      if (config == null) {
        return null;
      }
      return new ReadOnlyPartitionerConfig(config);
    }

    @Override
    public void setPartitionerConfig(PartitionerConfig partitionerConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isVersionSwapDeferred() {
      return this.delegate.isVersionSwapDeferred();
    }

    @Override
    public void setVersionSwapDeferred(boolean versionSwapDeferred) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getReplicationFactor() {
      return this.delegate.getReplicationFactor();
    }

    @Override
    public void setReplicationFactor(int replicationFactor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getMinActiveReplicas() {
      return this.delegate.getMinActiveReplicas();
    }

    @Override
    public String getNativeReplicationSourceFabric() {
      return this.delegate.getNativeReplicationSourceFabric();
    }

    @Override
    public void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isIncrementalPushEnabled() {
      return this.delegate.isIncrementalPushEnabled();
    }

    @Override
    public void setIncrementalPushEnabled(boolean incrementalPushEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSeparateRealTimeTopicEnabled() {
      return this.delegate.isSeparateRealTimeTopicEnabled();
    }

    @Override
    public void setSeparateRealTimeTopicEnabled(boolean separateRealTimeTopicEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBlobTransferEnabled() {
      return this.delegate.isBlobTransferEnabled();
    }

    @Override
    public void setBlobTransferEnabled(boolean blobTransferEnabled) {
      throw new UnsupportedOperationException("Blob transfer not supported");
    }

    @Override
    public boolean isUseVersionLevelIncrementalPushEnabled() {
      return this.delegate.isUseVersionLevelIncrementalPushEnabled();
    }

    @Override
    public void setUseVersionLevelIncrementalPushEnabled(boolean versionLevelIncrementalPushEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHybrid() {
      return this.delegate.isHybrid();
    }

    @Override
    public HybridStoreConfig getHybridStoreConfig() {
      HybridStoreConfig config = this.delegate.getHybridStoreConfig();
      if (config == null) {
        return null;
      }
      return new ReadOnlyHybridStoreConfig(config);
    }

    @Override
    public void setHybridStoreConfig(HybridStoreConfig hybridConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ViewConfig> getViewConfigs() {
      if (this.delegate.getViewConfigs() == null) {
        return new HashMap<>();
      } else {
        return Collections.unmodifiableMap(
            this.delegate.getViewConfigs()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new ReadOnlyViewConfig(e.getValue()))));
      }
    }

    @Override
    public void setViewConfigs(Map<String, ViewConfig> viewConfigList) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUseVersionLevelHybridConfig() {
      return this.delegate.isUseVersionLevelHybridConfig();
    }

    @Override
    public void setUseVersionLevelHybridConfig(boolean versionLevelHybridConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isActiveActiveReplicationEnabled() {
      return this.delegate.isActiveActiveReplicationEnabled();
    }

    @Override
    public void setActiveActiveReplicationEnabled(boolean activeActiveReplicationEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DataRecoveryVersionConfig getDataRecoveryVersionConfig() {
      DataRecoveryVersionConfig config = this.delegate.getDataRecoveryVersionConfig();
      if (config == null) {
        return null;
      }
      return new ReadOnlyDataRecoveryVersionConfig(config);
    }

    @Override
    public void setDataRecoveryVersionConfig(DataRecoveryVersionConfig dataRecoveryVersionConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getTargetSwapRegion() {
      return delegate.getTargetSwapRegion();
    }

    @Override
    public int getTargetSwapRegionWaitTime() {
      return delegate.getTargetSwapRegionWaitTime();
    }

    @Override
    public void setTargetSwapRegion(String targetRegion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setTargetSwapRegionWaitTime(int waitTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setIsDavinciHeartbeatReported(boolean isReported) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getIsDavinciHeartbeatReported() {
      return delegate.getIsDavinciHeartbeatReported();
    }

    @Override
    public boolean isGlobalRtDivEnabled() {
      return delegate.isGlobalRtDivEnabled();
    }

    @Override
    public void setGlobalRtDivEnabled(boolean globalRtDivEnabled) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setRepushSourceVersion(int version) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getRepushSourceVersion() {
      return this.delegate.getRepushSourceVersion();
    }

    @Override
    public int getRmdVersionId() {
      return this.delegate.getRmdVersionId();
    }

    @Override
    public void setRmdVersionId(int replicationMetadataVersionId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Version cloneVersion() {
      return this.delegate.cloneVersion();
    }

    @Override
    public String kafkaTopicName() {
      return this.delegate.kafkaTopicName();
    }

    @Override
    public StoreVersion dataModel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Version o) {
      return this.delegate.compareTo(o);
    }

    @Override
    public String toString() {
      return this.delegate.toString();
    }

    @Override
    public int hashCode() {
      return this.delegate.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReadOnlyVersion version = (ReadOnlyVersion) o;
      return this.delegate.equals(version.delegate);
    }
  }

  /**
   * A read-only wrapper of {@link SystemStoreAttributes}.
   */
  static class ReadOnlySystemStoreAttributes implements SystemStoreAttributes {
    private final SystemStoreAttributes delegate;

    public ReadOnlySystemStoreAttributes(SystemStoreAttributes delegate) {
      this.delegate = delegate;
    }

    @Override
    public SystemStoreProperties dataModel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLargestUsedVersionNumber() {
      return this.delegate.getLargestUsedVersionNumber();
    }

    @Override
    public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLargestUsedRTVersionNumber() {
      return this.delegate.getLargestUsedRTVersionNumber();
    }

    @Override
    public void setLargestUsedRTVersionNumber(int largestUsedRTVersionNumber) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getCurrentVersion() {
      return this.delegate.getCurrentVersion();
    }

    @Override
    public void setCurrentVersion(int currentVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLatestVersionPromoteToCurrentTimestamp() {
      return this.delegate.getLatestVersionPromoteToCurrentTimestamp();
    }

    @Override
    public void setLatestVersionPromoteToCurrentTimestamp(long timestamp) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Version> getVersions() {
      List<Version> versions = this.delegate.getVersions();
      return versions.stream().map(v -> new ReadOnlyVersion(v)).collect(Collectors.toList());
    }

    @Override
    public void setVersions(List<Version> versions) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SystemStoreAttributes clone() {
      return this.delegate.clone();
    }

    @Override
    public int hashCode() {
      return this.delegate.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReadOnlySystemStoreAttributes attributes = (ReadOnlySystemStoreAttributes) o;
      return this.delegate.equals(attributes.delegate);
    }
  }

  protected final Store delegate;

  public ReadOnlyStore(Store delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return this.delegate.getName();
  }

  @Override
  public String getOwner() {
    return this.delegate.getOwner();
  }

  @Override
  public void setOwner(String owner) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getCreatedTime() {
    return this.delegate.getCreatedTime();
  }

  @Override
  public int getCurrentVersion() {
    return this.delegate.getCurrentVersion();
  }

  @Override
  public void setCurrentVersion(int currentVersion) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCurrentVersionWithoutCheck(int currentVersion) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLowWatermark() {
    return this.delegate.getLowWatermark();
  }

  @Override
  public void setLowWatermark(long lowWatermark) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PersistenceType getPersistenceType() {
    return this.delegate.getPersistenceType();
  }

  @Override
  public void setPersistenceType(PersistenceType persistenceType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RoutingStrategy getRoutingStrategy() {
    return this.delegate.getRoutingStrategy();
  }

  @Override
  public ReadStrategy getReadStrategy() {
    return this.delegate.getReadStrategy();
  }

  @Override
  public OfflinePushStrategy getOffLinePushStrategy() {
    return this.delegate.getOffLinePushStrategy();
  }

  @Override
  public int getLargestUsedVersionNumber() {
    return this.delegate.getLargestUsedVersionNumber();
  }

  @Override
  public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLargestUsedRTVersionNumber() {
    return this.delegate.getLargestUsedRTVersionNumber();
  }

  @Override
  public void setLargestUsedRTVersionNumber(int largestUsedRTVersionNumber) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getStorageQuotaInByte() {
    return this.delegate.getStorageQuotaInByte();
  }

  @Override
  public void setStorageQuotaInByte(long storageQuotaInByte) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPartitionCount() {
    return this.delegate.getPartitionCount();
  }

  @Override
  public void setPartitionCount(int partitionCount) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PartitionerConfig getPartitionerConfig() {
    PartitionerConfig config = this.delegate.getPartitionerConfig();
    if (config == null) {
      return null;
    }
    return new ReadOnlyPartitionerConfig(config);
  }

  @Override
  public void setPartitionerConfig(PartitionerConfig value) {
    throw new UnsupportedOperationException();
  }

  public StoreProperties cloneStoreProperties() {
    StoreProperties storeProperties = new StoreProperties();

    storeProperties.setName(getName());
    storeProperties.setOwner(getOwner());
    storeProperties.setCreatedTime(getCreatedTime());
    storeProperties.setCurrentVersion(getCurrentVersion());
    storeProperties.setPartitionCount(getPartitionCount());
    storeProperties.setLowWatermark(getLowWatermark());
    storeProperties.setEnableWrites(isEnableWrites());
    storeProperties.setEnableReads(isEnableReads());
    storeProperties.setStorageQuotaInByte(getStorageQuotaInByte());
    storeProperties.setPersistenceType(getPersistenceType().value);
    storeProperties.setRoutingStrategy(getRoutingStrategy().value);
    storeProperties.setReadStrategy(getReadStrategy().value);
    storeProperties.setOfflinePushStrategy(getOffLinePushStrategy().value);
    storeProperties.setLargestUsedVersionNumber(getLargestUsedVersionNumber());
    storeProperties.setReadQuotaInCU(getReadQuotaInCU());
    storeProperties.setHybridConfig(convertHybridStoreConfig(getHybridStoreConfig()));
    storeProperties.setViews(convertViewConfigs(getViewConfigs()));
    storeProperties.setAccessControlled(isAccessControlled());
    storeProperties.setCompressionStrategy(getCompressionStrategy().getValue());
    storeProperties.setClientDecompressionEnabled(getClientDecompressionEnabled());
    storeProperties.setChunkingEnabled(isChunkingEnabled());
    storeProperties.setRmdChunkingEnabled(isRmdChunkingEnabled());
    storeProperties.setBatchGetLimit(getBatchGetLimit());
    storeProperties.setNumVersionsToPreserve(getNumVersionsToPreserve());
    storeProperties.setIncrementalPushEnabled(isIncrementalPushEnabled());
    storeProperties.setSeparateRealTimeTopicEnabled(isSeparateRealTimeTopicEnabled());
    storeProperties.setMigrating(isMigrating());
    storeProperties.setWriteComputationEnabled(isWriteComputationEnabled());
    storeProperties.setReadComputationEnabled(isReadComputationEnabled());
    storeProperties.setBootstrapToOnlineTimeoutInHours(getBootstrapToOnlineTimeoutInHours());
    // storeProperties.setLeaderFollowerModelEnabled(isLeaderFollowerModelEnabled());
    storeProperties.setNativeReplicationEnabled(isNativeReplicationEnabled());
    storeProperties.setReplicationMetadataVersionID(getRmdVersion());
    storeProperties.setPushStreamSourceAddress(getPushStreamSourceAddress());
    storeProperties.setBackupStrategy(getBackupStrategy().getValue());
    storeProperties.setSchemaAutoRegisteFromPushJobEnabled(isSchemaAutoRegisterFromPushJobEnabled());
    storeProperties.setLatestSuperSetValueSchemaId(getLatestSuperSetValueSchemaId());
    storeProperties.setHybridStoreDiskQuotaEnabled(isHybridStoreDiskQuotaEnabled());
    storeProperties.setStoreMetaSystemStoreEnabled(isStoreMetaSystemStoreEnabled());
    storeProperties.setStoreMetadataSystemStoreEnabled(isStoreMetadataSystemStoreEnabled());
    storeProperties.setEtlConfig(convertETLStoreConfig(getEtlStoreConfig()));
    storeProperties.setPartitionerConfig(convertPartitionerConfig(getPartitionerConfig()));
    // storeProperties.setIncrementalPushPolicy(IncrementalPushPolicy());
    storeProperties.setLatestVersionPromoteToCurrentTimestamp(getLatestVersionPromoteToCurrentTimestamp());
    storeProperties.setBackupVersionRetentionMs(getBackupVersionRetentionMs());
    storeProperties.setReplicationFactor(getReplicationFactor());
    storeProperties.setMigrationDuplicateStore(isMigrationDuplicateStore());
    storeProperties.setNativeReplicationSourceFabric(getNativeReplicationSourceFabric());
    storeProperties.setDaVinciPushStatusStoreEnabled(isDaVinciPushStatusStoreEnabled());
    storeProperties.setActiveActiveReplicationEnabled(isActiveActiveReplicationEnabled());
    // storeProperties.setApplyTargetVersionFilterForIncPush(isApplyTargetVersionFilterForIncPush());
    storeProperties.setCompactionEnabled(isCompactionEnabled());
    storeProperties.setCompactionThresholdMilliseconds(getCompactionThresholdMilliseconds());
    storeProperties.setMinCompactionLagSeconds(getMinCompactionLagSeconds());
    storeProperties.setMaxCompactionLagSeconds(getMaxCompactionLagSeconds());
    storeProperties.setMaxRecordSizeBytes(getMaxRecordSizeBytes());
    storeProperties.setMaxNearlineRecordSizeBytes(getMaxNearlineRecordSizeBytes());
    storeProperties.setUnusedSchemaDeletionEnabled(isUnusedSchemaDeletionEnabled());
    storeProperties.setVersions(convertVersions(getVersions()));
    storeProperties.setSystemStores(convertSystemStores(getSystemStores()));
    storeProperties.setStorageNodeReadQuotaEnabled(isStorageNodeReadQuotaEnabled());
    storeProperties.setBlobTransferEnabled(isBlobTransferEnabled());
    storeProperties.setNearlineProducerCompressionEnabled(isNearlineProducerCompressionEnabled());
    storeProperties.setNearlineProducerCountPerWriter(getNearlineProducerCountPerWriter());
    storeProperties.setTargetSwapRegion(getTargetSwapRegion());
    storeProperties.setTargetSwapRegionWaitTime(getTargetSwapRegionWaitTime());
    storeProperties.setIsDaVinciHeartBeatReported(getIsDavinciHeartbeatReported());

    return storeProperties;
  }

  @Override
  public boolean isEnableWrites() {
    return this.delegate.isEnableWrites();
  }

  @Override
  public void setEnableWrites(boolean enableWrites) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEnableReads() {
    return this.delegate.isEnableReads();
  }

  @Override
  public void setEnableReads(boolean enableReads) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getReadQuotaInCU() {
    return this.delegate.getReadQuotaInCU();
  }

  @Override
  public void setReadQuotaInCU(long readQuotaInCU) {
    throw new UnsupportedOperationException();
  }

  @Override
  public HybridStoreConfig getHybridStoreConfig() {
    HybridStoreConfig config = this.delegate.getHybridStoreConfig();
    if (config == null) {
      return null;
    }
    return new ReadOnlyHybridStoreConfig(config);
  }

  @Override
  public void setHybridStoreConfig(HybridStoreConfig hybridStoreConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, ViewConfig> getViewConfigs() {

    return Collections.unmodifiableMap(
        this.delegate.getViewConfigs()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new ReadOnlyViewConfig(e.getValue()))));
  }

  @Override
  public void setViewConfigs(Map<String, ViewConfig> viewConfigList) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isHybrid() {
    return this.delegate.isHybrid();
  }

  @Override
  public CompressionStrategy getCompressionStrategy() {
    return this.delegate.getCompressionStrategy();
  }

  @Override
  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getClientDecompressionEnabled() {
    return this.delegate.getClientDecompressionEnabled();
  }

  @Override
  public void setClientDecompressionEnabled(boolean clientDecompressionEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isChunkingEnabled() {
    return this.delegate.isChunkingEnabled();
  }

  @Override
  public void setChunkingEnabled(boolean chunkingEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isRmdChunkingEnabled() {
    return this.delegate.isRmdChunkingEnabled();
  }

  @Override
  public void setRmdChunkingEnabled(boolean rmdChunkingEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBatchGetLimit() {
    return this.delegate.getBatchGetLimit();
  }

  @Override
  public void setBatchGetLimit(int batchGetLimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isIncrementalPushEnabled() {
    return this.delegate.isIncrementalPushEnabled();
  }

  @Override
  public void setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSeparateRealTimeTopicEnabled() {
    return this.delegate.isSeparateRealTimeTopicEnabled();
  }

  @Override
  public void setSeparateRealTimeTopicEnabled(boolean separateRealTimeTopicEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAccessControlled() {
    return this.delegate.isAccessControlled();
  }

  @Override
  public void setAccessControlled(boolean accessControlled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isMigrating() {
    return this.delegate.isMigrating();
  }

  @Override
  public void setMigrating(boolean migrating) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumVersionsToPreserve() {
    return this.delegate.getNumVersionsToPreserve();
  }

  @Override
  public void setNumVersionsToPreserve(int numVersionsToPreserve) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWriteComputationEnabled() {
    return this.delegate.isWriteComputationEnabled();
  }

  @Override
  public void setWriteComputationEnabled(boolean writeComputationEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadComputationEnabled() {
    return this.delegate.isReadComputationEnabled();
  }

  @Override
  public void setReadComputationEnabled(boolean readComputationEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBootstrapToOnlineTimeoutInHours() {
    return this.delegate.getBootstrapToOnlineTimeoutInHours();
  }

  @Override
  public void setBootstrapToOnlineTimeoutInHours(int bootstrapToOnlineTimeoutInHours) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getPushStreamSourceAddress() {
    return this.delegate.getPushStreamSourceAddress();
  }

  @Override
  public void setPushStreamSourceAddress(String sourceAddress) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNativeReplicationEnabled() {
    return this.delegate.isNativeReplicationEnabled();
  }

  @Override
  public int getRmdVersion() {
    return this.delegate.getRmdVersion();
  }

  @Override
  public void setRmdVersion(int rmdVersion) {
    throw new UnsupportedOperationException("This class is read-only. Hence set/write operation is not allowed");
  }

  @Override
  public void setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BackupStrategy getBackupStrategy() {
    return this.delegate.getBackupStrategy();
  }

  @Override
  public void setBackupStrategy(BackupStrategy value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSchemaAutoRegisterFromPushJobEnabled() {
    return this.delegate.isSchemaAutoRegisterFromPushJobEnabled();
  }

  @Override
  public void setSchemaAutoRegisterFromPushJobEnabled(boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLatestSuperSetValueSchemaId() {
    return this.delegate.getLatestSuperSetValueSchemaId();
  }

  @Override
  public void setLatestSuperSetValueSchemaId(int valueSchemaId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isHybridStoreDiskQuotaEnabled() {
    return this.delegate.isHybridStoreDiskQuotaEnabled();
  }

  @Override
  public void setHybridStoreDiskQuotaEnabled(boolean enabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ETLStoreConfig getEtlStoreConfig() {
    ETLStoreConfig config = this.delegate.getEtlStoreConfig();
    if (config == null) {
      return null;
    }
    return new ReadOnlyETLStoreConfig(config);
  }

  @Override
  public void setEtlStoreConfig(ETLStoreConfig etlStoreConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isStoreMetadataSystemStoreEnabled() {
    return this.delegate.isStoreMetadataSystemStoreEnabled();
  }

  @Override
  public void setStoreMetadataSystemStoreEnabled(boolean storeMetadataSystemStoreEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isStoreMetaSystemStoreEnabled() {
    return this.delegate.isStoreMetaSystemStoreEnabled();
  }

  @Override
  public void setStoreMetaSystemStoreEnabled(boolean storeMetaSystemStoreEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLatestVersionPromoteToCurrentTimestamp() {
    return this.delegate.getLatestVersionPromoteToCurrentTimestamp();
  }

  @Override
  public void setLatestVersionPromoteToCurrentTimestamp(long latestVersionPromoteToCurrentTimestamp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getBackupVersionRetentionMs() {
    return this.delegate.getBackupVersionRetentionMs();
  }

  @Override
  public void setBackupVersionRetentionMs(long backupVersionRetentionMs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getRetentionTime() {
    return this.delegate.getRetentionTime();
  }

  @Override
  public int getReplicationFactor() {
    return this.delegate.getReplicationFactor();
  }

  @Override
  public void setReplicationFactor(int replicationFactor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isMigrationDuplicateStore() {
    return this.delegate.isMigrationDuplicateStore();
  }

  @Override
  public void setMigrationDuplicateStore(boolean migrationDuplicateStore) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getNativeReplicationSourceFabric() {
    return this.delegate.getNativeReplicationSourceFabric();
  }

  @Override
  public void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isActiveActiveReplicationEnabled() {
    return this.delegate.isActiveActiveReplicationEnabled();
  }

  @Override
  public void setActiveActiveReplicationEnabled(boolean activeActiveReplicationEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, SystemStoreAttributes> getSystemStores() {
    Map<String, SystemStoreAttributes> systemStores = new HashMap<>();
    this.delegate.getSystemStores()
        .forEach((name, systemStore) -> systemStores.put(name, new ReadOnlySystemStoreAttributes(systemStore)));

    return systemStores;
  }

  @Override
  public void setSystemStores(Map<String, SystemStoreAttributes> systemStores) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putSystemStore(VeniceSystemStoreType systemStoreType, SystemStoreAttributes systemStoreAttributes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDaVinciPushStatusStoreEnabled() {
    return this.delegate.isDaVinciPushStatusStoreEnabled();
  }

  @Override
  public void setDaVinciPushStatusStoreEnabled(boolean daVinciPushStatusStoreEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Store cloneStore() {
    return this.delegate.cloneStore();
  }

  @Override
  public List<Version> getVersions() {
    List<Version> versions = this.delegate.getVersions();
    if (versions.isEmpty()) {
      return versions;
    }

    return versions.stream().map(v -> new ReadOnlyVersion(v)).collect(Collectors.toList());
  }

  @Override
  public void setVersions(List<Version> versions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addVersion(Version version) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addVersion(Version version, boolean isClonedVersion, int currentRTVersionNumber) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void forceAddVersion(Version version, boolean isClonedVersion) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void checkDisableStoreWrite(String action, int version) {
    this.delegate.checkDisableStoreWrite(action, version);
  }

  @Override
  public Version deleteVersion(int versionNumber) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsVersion(int versionNumber) {
    return this.delegate.containsVersion(versionNumber);
  }

  @Override
  public void updateVersionStatus(int versionNumber, VersionStatus status) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int peekNextVersionNumber() {
    return this.delegate.peekNextVersionNumber();
  }

  @Override
  @Nullable
  public Version getVersion(int versionNumber) {
    Version version = this.delegate.getVersion(versionNumber);
    if (version != null) {
      version = new ReadOnlyVersion(version);
    }
    return version;
  }

  @Override
  @Nonnull
  public Version getVersionOrThrow(int versionNumber) throws StoreVersionNotFoundException {
    return new ReadOnlyVersion(this.delegate.getVersionOrThrow(versionNumber));
  }

  @Override
  public VersionStatus getVersionStatus(int versionNumber) {
    return this.delegate.getVersionStatus(versionNumber);
  }

  @Override
  public List<Version> retrieveVersionsToDelete(int clusterNumVersionsToPreserve) {
    return this.delegate.retrieveVersionsToDelete(clusterNumVersionsToPreserve)
        .stream()
        .map(v -> new ReadOnlyVersion(v))
        .collect(Collectors.toList());
  }

  @Override
  public boolean isSystemStore() {
    return this.delegate.isSystemStore();
  }

  @Override
  public void fixMissingFields() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isStorageNodeReadQuotaEnabled() {
    return this.delegate.isStorageNodeReadQuotaEnabled();
  }

  @Override
  public void setStorageNodeReadQuotaEnabled(boolean storageNodeReadQuotaEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCompactionEnabled() {
    return this.delegate.isCompactionEnabled();
  }

  @Override
  public void setCompactionEnabled(boolean compactionEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getCompactionThresholdMilliseconds() {
    return this.delegate.getCompactionThresholdMilliseconds();
  }

  @Override
  public void setCompactionThresholdMilliseconds(long compactionThreshold) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getMinCompactionLagSeconds() {
    return this.delegate.getMinCompactionLagSeconds();
  }

  @Override
  public void setMinCompactionLagSeconds(long minCompactionLagSeconds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getMaxCompactionLagSeconds() {
    return this.delegate.getMaxCompactionLagSeconds();
  }

  @Override
  public void setMaxCompactionLagSeconds(long maxCompactionLagSeconds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxRecordSizeBytes() {
    return this.delegate.getMaxRecordSizeBytes();
  }

  @Override
  public void setMaxRecordSizeBytes(int maxRecordSizeBytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxNearlineRecordSizeBytes() {
    return this.delegate.getMaxNearlineRecordSizeBytes();
  }

  @Override
  public void setMaxNearlineRecordSizeBytes(int maxNearlineRecordSizeBytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setUnusedSchemaDeletionEnabled(boolean unusedSchemaDeletionEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isUnusedSchemaDeletionEnabled() {
    return this.delegate.isUnusedSchemaDeletionEnabled();
  }

  @Override
  public void setBlobTransferEnabled(boolean blobTransferEnabled) {
    throw new UnsupportedOperationException("Blob transfer not supported");
  }

  @Override
  public boolean isBlobTransferEnabled() {
    return this.delegate.isBlobTransferEnabled();
  }

  @Override
  public boolean isNearlineProducerCompressionEnabled() {
    return delegate.isNearlineProducerCompressionEnabled();
  }

  @Override
  public void setNearlineProducerCompressionEnabled(boolean compressionEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNearlineProducerCountPerWriter() {
    return delegate.getNearlineProducerCountPerWriter();
  }

  @Override
  public void setNearlineProducerCountPerWriter(int producerCnt) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getTargetSwapRegion() {
    return delegate.getTargetSwapRegion();
  }

  @Override
  public int getTargetSwapRegionWaitTime() {
    return delegate.getTargetSwapRegionWaitTime();
  }

  @Override
  public void setTargetSwapRegion(String targetRegion) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTargetSwapRegionWaitTime(int waitTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setIsDavinciHeartbeatReported(boolean isReported) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getIsDavinciHeartbeatReported() {
    return delegate.getIsDavinciHeartbeatReported();
  }

  @Override
  public void updateVersionForDaVinciHeartbeat(int versionNumber, boolean reported) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isGlobalRtDivEnabled() {
    return delegate.isGlobalRtDivEnabled();
  }

  @Override
  public void setGlobalRtDivEnabled(boolean globalRtDivEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTTLRepushEnabled() {
    return delegate.isTTLRepushEnabled();
  }

  @Override
  public void setTTLRepushEnabled(boolean ttlRepushEnabled) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return this.delegate.toString();
  }

  @Override
  public int hashCode() {
    return this.delegate.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReadOnlyStore store = (ReadOnlyStore) o;
    return this.delegate.equals(store.delegate);
  }

  private static StoreETLConfig convertETLStoreConfig(ETLStoreConfig etlStoreConfig) {
    if (etlStoreConfig == null) {
      return null;
    }
    StoreETLConfig storeETLConfig = new StoreETLConfig();

    storeETLConfig.setEtledUserProxyAccount(etlStoreConfig.getEtledUserProxyAccount());
    storeETLConfig.setRegularVersionETLEnabled(etlStoreConfig.isRegularVersionETLEnabled());
    storeETLConfig.setFutureVersionETLEnabled(etlStoreConfig.isFutureVersionETLEnabled());

    return storeETLConfig;
  }

  private static StorePartitionerConfig convertPartitionerConfig(PartitionerConfig partitionerConfig) {
    if (partitionerConfig == null) {
      return null;
    }
    StorePartitionerConfig storePartitionerConfig = new StorePartitionerConfig();

    // Partitioner Class
    storePartitionerConfig.setPartitionerClass(partitionerConfig.getPartitionerClass());

    // Partitioner Params
    Map<String, String> partitionerConfigParam = partitionerConfig.getPartitionerParams();
    Map<CharSequence, CharSequence> storePartitionerConfigParams = new HashMap<>(partitionerConfigParam);
    storePartitionerConfig.setPartitionerParams(storePartitionerConfigParams);

    // Amplification Factor
    storePartitionerConfig.setAmplificationFactor(partitionerConfig.getAmplificationFactor());

    return storePartitionerConfig;
  }

  private static StoreHybridConfig convertHybridStoreConfig(HybridStoreConfig hybridStoreConfig) {
    if (hybridStoreConfig == null) {
      return null;
    }
    StoreHybridConfig storeHybridConfig = new StoreHybridConfig();

    storeHybridConfig.setRewindTimeInSeconds(hybridStoreConfig.getRewindTimeInSeconds());
    storeHybridConfig.setOffsetLagThresholdToGoOnline(hybridStoreConfig.getOffsetLagThresholdToGoOnline());
    storeHybridConfig.setProducerTimestampLagThresholdToGoOnlineInSeconds(
        hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds());
    storeHybridConfig.setDataReplicationPolicy(hybridStoreConfig.getDataReplicationPolicy().getValue());
    storeHybridConfig.setBufferReplayPolicy(hybridStoreConfig.getBufferReplayPolicy().getValue());
    storeHybridConfig.setRealTimeTopicName(hybridStoreConfig.getRealTimeTopicName());

    return storeHybridConfig;
  }

  private static Map<CharSequence, StoreViewConfig> convertViewConfigs(Map<String, ViewConfig> viewConfigs) {
    Map<CharSequence, StoreViewConfig> storeViewConfigs = new HashMap<>();

    for (Map.Entry<String, ViewConfig> entry: viewConfigs.entrySet()) {
      ViewConfig viewConfig = entry.getValue();
      StoreViewConfig storeViewConfig = new StoreViewConfig();

      storeViewConfig.setViewClassName(viewConfig.getViewClassName());
      storeViewConfig.setViewParameters(new HashMap<>(viewConfig.getViewParameters()));

      storeViewConfigs.put(entry.getKey(), storeViewConfig);
    }

    return storeViewConfigs;
  }

  private static Map<String, StoreViewConfig> convertViewConfigsStringMap(Map<String, ViewConfig> viewConfigs) {
    Map<String, StoreViewConfig> storeViewConfigsStringMap = new HashMap<>();

    Map<CharSequence, StoreViewConfig> storeViewConfigs = convertViewConfigs(viewConfigs);
    for (Map.Entry<CharSequence, StoreViewConfig> entry: storeViewConfigs.entrySet()) {
      storeViewConfigsStringMap.put(entry.getKey().toString(), entry.getValue());
    }

    return storeViewConfigsStringMap;
  }

  private static List<StoreVersion> convertVersions(List<Version> versions) {
    List<StoreVersion> storeVersions = new ArrayList<>(versions.size());
    for (Version version: versions) {
      StoreVersion storeVersion = convertVersion(version);
      if (storeVersion != null) {
        storeVersions.add(storeVersion);
      }
    }
    return storeVersions;
  }

  private static StoreVersion convertVersion(Version version) {
    if (version == null) {
      return null;
    }
    StoreVersion storeVersion = new StoreVersion();

    storeVersion.setStoreName(version.getStoreName());
    storeVersion.setNumber(version.getNumber());
    storeVersion.setCreatedTime(version.getCreatedTime());
    storeVersion.setStatus(version.getStatus().getValue());
    storeVersion.setPushJobId(version.getPushJobId());
    storeVersion.setCompressionStrategy(version.getCompressionStrategy().getValue());
    storeVersion.setLeaderFollowerModelEnabled(version.isLeaderFollowerModelEnabled());
    storeVersion.setNativeReplicationEnabled(version.isNativeReplicationEnabled());
    storeVersion.setPushStreamSourceAddress(version.getPushStreamSourceAddress());
    // storeVersion.setBufferReplayEnabledForHybrid();
    storeVersion.setChunkingEnabled(version.isChunkingEnabled());
    storeVersion.setRmdChunkingEnabled(version.isRmdChunkingEnabled());
    storeVersion.setPushType(version.getPushType().getValue());
    storeVersion.setPartitionCount(version.getPartitionCount());
    storeVersion.setPartitionerConfig(convertPartitionerConfig(version.getPartitionerConfig()));
    // storeVersion.setIncrementalPushPolicy(version.getIncrementalPushPolicy());
    storeVersion.setReplicationFactor(version.getReplicationFactor());
    storeVersion.setNativeReplicationSourceFabric(version.getNativeReplicationSourceFabric());
    storeVersion.setIncrementalPushEnabled(version.isIncrementalPushEnabled());
    storeVersion.setSeparateRealTimeTopicEnabled(version.isSeparateRealTimeTopicEnabled());
    storeVersion.setBlobTransferEnabled(version.isBlobTransferEnabled());
    storeVersion.setUseVersionLevelIncrementalPushEnabled(version.isUseVersionLevelIncrementalPushEnabled());
    storeVersion.setHybridConfig(convertHybridStoreConfig(version.getHybridStoreConfig()));
    storeVersion.setUseVersionLevelHybridConfig(version.isUseVersionLevelHybridConfig());
    storeVersion.setActiveActiveReplicationEnabled(version.isActiveActiveReplicationEnabled());
    storeVersion.setTimestampMetadataVersionId(version.getRmdVersionId());
    storeVersion.setDataRecoveryConfig(convertDataRecoveryVersionConfig(version.getDataRecoveryVersionConfig()));
    storeVersion.setDeferVersionSwap(version.isVersionSwapDeferred());
    storeVersion.setRepushSourceVersion(version.getRepushSourceVersion());
    storeVersion.setTargetSwapRegion(version.getTargetSwapRegion());
    storeVersion.setTargetSwapRegionWaitTime(version.getTargetSwapRegionWaitTime());
    storeVersion.setIsDaVinciHeartBeatReported(version.getIsDavinciHeartbeatReported());
    storeVersion.setGlobalRtDivEnabled(version.isGlobalRtDivEnabled());
    storeVersion.setViews(convertViewConfigsStringMap(version.getViewConfigs()));

    return storeVersion;
  }

  private static DataRecoveryConfig convertDataRecoveryVersionConfig(
      DataRecoveryVersionConfig dataRecoveryVersionConfig) {
    if (dataRecoveryVersionConfig == null) {
      return null;
    }
    DataRecoveryConfig dataRecoveryConfig = new DataRecoveryConfig();

    dataRecoveryConfig.setDataRecoverySourceFabric(dataRecoveryVersionConfig.getDataRecoverySourceFabric());
    dataRecoveryConfig.setIsDataRecoveryComplete(dataRecoveryConfig.getIsDataRecoveryComplete());
    dataRecoveryConfig.setDataRecoverySourceVersionNumber(dataRecoveryConfig.getDataRecoverySourceVersionNumber());

    return dataRecoveryConfig;
  }

  private static Map<CharSequence, SystemStoreProperties> convertSystemStores(
      Map<String, SystemStoreAttributes> systemStoreAttributesMap) {

    Map<CharSequence, SystemStoreProperties> systemStorePropertiesMap = new HashMap<>();
    for (Map.Entry<String, SystemStoreAttributes> entry: systemStoreAttributesMap.entrySet()) {
      SystemStoreAttributes systemStoreAttributes = entry.getValue();
      SystemStoreProperties systemStoreProperties = new SystemStoreProperties();

      systemStoreProperties.setCurrentVersion(systemStoreAttributes.getCurrentVersion());
      systemStoreProperties.setLargestUsedVersionNumber(systemStoreAttributes.getLargestUsedVersionNumber());
      systemStoreProperties.setVersions(convertVersions(systemStoreAttributes.getVersions()));
      systemStoreProperties
          .setLatestVersionPromoteToCurrentTimestamp(systemStoreAttributes.getLatestVersionPromoteToCurrentTimestamp());

      systemStorePropertiesMap.put(entry.getKey(), systemStoreProperties);
    }

    return systemStorePropertiesMap;
  }
}
