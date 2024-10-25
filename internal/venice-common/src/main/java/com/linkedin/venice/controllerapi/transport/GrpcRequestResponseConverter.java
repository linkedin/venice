package com.linkedin.venice.controllerapi.transport;

import com.google.rpc.ErrorInfo;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataRecoveryVersionConfig;
import com.linkedin.venice.meta.DataRecoveryVersionConfigImpl;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.ETLStoreConfigImpl;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.protocols.BackupStrategyGrpc;
import com.linkedin.venice.protocols.BufferReplayPolicyGrpc;
import com.linkedin.venice.protocols.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.CompressionStrategyGrpc;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.DataRecoveryVersionConfigGrpc;
import com.linkedin.venice.protocols.DataReplicationPolicyGrpc;
import com.linkedin.venice.protocols.ETLStoreConfigGrpc;
import com.linkedin.venice.protocols.EmptyPushGrpcResponse;
import com.linkedin.venice.protocols.HybridStoreConfigGrpc;
import com.linkedin.venice.protocols.PartitionerConfigGrpc;
import com.linkedin.venice.protocols.PushTypeGrpc;
import com.linkedin.venice.protocols.StoreInfoGrpc;
import com.linkedin.venice.protocols.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.VersionGrpc;
import com.linkedin.venice.protocols.VersionStatusGrpc;
import com.linkedin.venice.protocols.ViewConfigGrpc;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GrpcRequestResponseConverter {
  public static CreateStoreGrpcRequest toGrpcCreateStoreGrpcRequest(NewStoreRequest newStoreRequest) {
    CreateStoreGrpcRequest.Builder builder = CreateStoreGrpcRequest.newBuilder()
        .setClusterStoreInfo(
            ClusterStoreGrpcInfo.newBuilder()
                .setClusterName(newStoreRequest.getClusterName())
                .setStoreName(newStoreRequest.getStoreName())
                .build())
        .setKeySchema(newStoreRequest.getKeySchema())
        .setValueSchema(newStoreRequest.getValueSchema())
        .setIsSystemStore(newStoreRequest.isSystemStore());

    // For optional fields, only set if they are not null
    if (newStoreRequest.getOwner() != null) {
      builder.setOwner(newStoreRequest.getOwner());
    }
    if (newStoreRequest.getAccessPermissions() != null) {
      builder.setAccessPermission(newStoreRequest.getAccessPermissions());
    }
    return builder.build();
  }

  public static NewStoreResponse fromGrpcResponse(CreateStoreGrpcResponse grpcResponse) {
    NewStoreResponse response = new NewStoreResponse();
    response.setOwner(grpcResponse.getOwner());
    return response;
  }

  public static NewStoreRequest convertGrpcRequestToNewStoreRequest(CreateStoreGrpcRequest grpcRequest) {
    return new NewStoreRequest(
        grpcRequest.getClusterStoreInfo().getClusterName(),
        grpcRequest.getClusterStoreInfo().getStoreName(),
        grpcRequest.hasOwner() ? grpcRequest.getOwner() : null,
        grpcRequest.getKeySchema(),
        grpcRequest.getValueSchema(),
        grpcRequest.hasAccessPermission() ? grpcRequest.getAccessPermission() : null,
        grpcRequest.getIsSystemStore());
  }

  // UpdateAclForStoreRequest
  public static UpdateAclForStoreRequest convertGrpcRequestToUpdateAclForStoreRequest(
      UpdateAclForStoreGrpcRequest grpcRequest) {
    return new UpdateAclForStoreRequest(
        grpcRequest.getClusterStoreInfo().getClusterName(),
        grpcRequest.getClusterStoreInfo().getStoreName(),
        grpcRequest.getAccessPermissions());
  }

  public static ClusterStoreGrpcInfo getClusterStoreGrpcInfo(ControllerResponse response) {
    ClusterStoreGrpcInfo.Builder builder = ClusterStoreGrpcInfo.newBuilder();
    if (response.getCluster() != null) {
      builder.setClusterName(response.getCluster());
    }
    if (response.getName() != null) {
      builder.setStoreName(response.getName());
    }
    return builder.build();
  }

  public static ClusterStoreGrpcInfo getClusterStoreGrpcInfo(ControllerRequest request) {
    ClusterStoreGrpcInfo.Builder builder = ClusterStoreGrpcInfo.newBuilder();
    if (request.getClusterName() != null) {
      builder.setClusterName(request.getClusterName());
    }
    if (request.getStoreName() != null) {
      builder.setStoreName(request.getStoreName());
    }
    return builder.build();
  }

  public static ControllerRequest getControllerRequest(ClusterStoreGrpcInfo clusterStoreGrpcInfo) {
    return new ControllerRequest(clusterStoreGrpcInfo.getClusterName(), clusterStoreGrpcInfo.getStoreName());
  }

  /**
   * Handles the gRPC exception by extracting the error details and returning a VeniceException.
   *
   * @param e the gRPC StatusRuntimeException
   * @return a VeniceException with the extracted error details
   */
  public static VeniceClientException handleGrpcError(StatusRuntimeException e) {
    com.google.rpc.Status status = StatusProto.fromThrowable(e);

    if (status != null) {
      // Extract gRPC status code and message
      int errorCode = status.getCode();
      StringBuilder errorMessage = new StringBuilder(status.getMessage());

      // Process ErrorInfo if present in the details
      for (com.google.protobuf.Any detail: status.getDetailsList()) {
        if (detail.is(ErrorInfo.class)) {
          try {
            ErrorInfo errorInfo = detail.unpack(ErrorInfo.class);
            // Append the error info to the error message
            errorMessage.append(" Reason: ")
                .append(errorInfo.getReason())
                .append(", Metadata: ")
                .append(errorInfo.getMetadataMap());
          } catch (Exception unpackException) {
            // If unpacking fails, include that info in the message
            errorMessage.append(". Failed to unpack error details: ").append(unpackException.getMessage());
          }
        }
      }

      // Return a VeniceException with error code and message
      return new VeniceClientException(
          "gRPC error occurred. Error code: " + errorCode + ", Error message: " + errorMessage);
    }

    // Return a generic VeniceException if no detailed gRPC status is found
    return new VeniceClientException("An unknown gRPC error occurred. Error code: " + Code.UNKNOWN.name());
  }

  public static VersionStatusGrpc fromVersionStatus(VersionStatus status) {
    switch (status) {
      case NOT_CREATED:
        return VersionStatusGrpc.NOT_CREATED;
      case STARTED:
        return VersionStatusGrpc.STARTED;
      case PUSHED:
        return VersionStatusGrpc.PUSHED;
      case ONLINE:
        return VersionStatusGrpc.ONLINE;
      case ERROR:
        return VersionStatusGrpc.ERROR;
      case CREATED:
        return VersionStatusGrpc.CREATED;
      case PARTIALLY_ONLINE:
        return VersionStatusGrpc.PARTIALLY_ONLINE;
      case KILLED:
        return VersionStatusGrpc.KILLED;
      default:
        throw new IllegalArgumentException("Unknown version status: " + status);
    }
  }

  public static VersionStatus toVersionStatus(VersionStatusGrpc statusGrpc) {
    switch (statusGrpc) {
      case NOT_CREATED:
        return VersionStatus.NOT_CREATED;
      case STARTED:
        return VersionStatus.STARTED;
      case PUSHED:
        return VersionStatus.PUSHED;
      case ONLINE:
        return VersionStatus.ONLINE;
      case ERROR:
        return VersionStatus.ERROR;
      case CREATED:
        return VersionStatus.CREATED;
      case PARTIALLY_ONLINE:
        return VersionStatus.PARTIALLY_ONLINE;
      case KILLED:
        return VersionStatus.KILLED;
      default:
        throw new IllegalArgumentException("Unknown version status: " + statusGrpc);
    }
  }

  public static CompressionStrategyGrpc fromCompressionStrategy(CompressionStrategy compressionStrategy) {
    switch (compressionStrategy) {
      case GZIP:
        return CompressionStrategyGrpc.GZIP;
      case ZSTD:
        return CompressionStrategyGrpc.ZSTD;
      case ZSTD_WITH_DICT:
        return CompressionStrategyGrpc.ZSTD_WITH_DICT;
      case NO_OP:
        return CompressionStrategyGrpc.NO_OP;
      default:
        throw new IllegalArgumentException("Unknown compression strategy: " + compressionStrategy);
    }
  }

  public static CompressionStrategy toCompressionStrategy(CompressionStrategyGrpc compressionStrategyGrpc) {
    switch (compressionStrategyGrpc) {
      case GZIP:
        return CompressionStrategy.GZIP;
      case ZSTD:
        return CompressionStrategy.ZSTD;
      case ZSTD_WITH_DICT:
        return CompressionStrategy.ZSTD_WITH_DICT;
      case NO_OP:
        return CompressionStrategy.NO_OP;
      default:
        throw new IllegalArgumentException("Unknown compression strategy: " + compressionStrategyGrpc);
    }
  }

  public static PushTypeGrpc fromPushType(Version.PushType pushType) {
    switch (pushType) {
      case BATCH:
        return PushTypeGrpc.BATCH;
      case STREAM_REPROCESSING:
        return PushTypeGrpc.STREAM_REPROCESSING;
      case STREAM:
        return PushTypeGrpc.STREAM;
      case INCREMENTAL:
        return PushTypeGrpc.INCREMENTAL;
      default:
        throw new IllegalArgumentException("Unknown push type: " + pushType);
    }
  }

  public static Version.PushType toPushType(PushTypeGrpc pushTypeGrpc) {
    switch (pushTypeGrpc) {
      case BATCH:
        return Version.PushType.BATCH;
      case STREAM_REPROCESSING:
        return Version.PushType.STREAM_REPROCESSING;
      case STREAM:
        return Version.PushType.STREAM;
      case INCREMENTAL:
        return Version.PushType.INCREMENTAL;
      default:
        throw new IllegalArgumentException("Unknown push type: " + pushTypeGrpc);
    }
  }

  static DataReplicationPolicyGrpc fromDataReplicationPolicy(DataReplicationPolicy dataReplicationPolicy) {
    switch (dataReplicationPolicy) {
      case AGGREGATE:
        return DataReplicationPolicyGrpc.AGGREGATE;
      case NON_AGGREGATE:
        return DataReplicationPolicyGrpc.NON_AGGREGATE;
      case NONE:
        return DataReplicationPolicyGrpc.NONE;
      case ACTIVE_ACTIVE:
        return DataReplicationPolicyGrpc.ACTIVE_ACTIVE;
      default:
        throw new IllegalArgumentException("Unknown data replication policy: " + dataReplicationPolicy);
    }
  }

  public static DataReplicationPolicy toDataReplicationPolicy(DataReplicationPolicyGrpc policyGrpc) {
    switch (policyGrpc) {
      case AGGREGATE:
        return DataReplicationPolicy.AGGREGATE;
      case NON_AGGREGATE:
        return DataReplicationPolicy.NON_AGGREGATE;
      case NONE:
        return DataReplicationPolicy.NONE;
      case ACTIVE_ACTIVE:
        return DataReplicationPolicy.ACTIVE_ACTIVE;
      default:
        throw new IllegalArgumentException("Unknown data replication policy: " + policyGrpc);
    }
  }

  static BufferReplayPolicyGrpc fromBufferReplayPolicy(BufferReplayPolicy bufferReplayPolicy) {
    switch (bufferReplayPolicy) {
      case REWIND_FROM_EOP:
        return BufferReplayPolicyGrpc.REWIND_FROM_EOP;
      case REWIND_FROM_SOP:
        return BufferReplayPolicyGrpc.REWIND_FROM_SOP;
      default:
        throw new IllegalArgumentException("Unknown buffer replay policy: " + bufferReplayPolicy);
    }
  }

  public static BufferReplayPolicy toBufferReplayPolicy(BufferReplayPolicyGrpc policyGrpc) {
    switch (policyGrpc) {
      case REWIND_FROM_EOP:
        return BufferReplayPolicy.REWIND_FROM_EOP;
      case REWIND_FROM_SOP:
        return BufferReplayPolicy.REWIND_FROM_SOP;
      default:
        throw new IllegalArgumentException("Unknown buffer replay policy: " + policyGrpc);
    }
  }

  public static BackupStrategyGrpc fromBackupStrategy(BackupStrategy backupStrategy) {
    switch (backupStrategy) {
      case KEEP_MIN_VERSIONS:
        return BackupStrategyGrpc.KEEP_MIN_VERSIONS;
      case DELETE_ON_NEW_PUSH_START:
        return BackupStrategyGrpc.DELETE_ON_NEW_PUSH_START;
      default:
        throw new IllegalArgumentException("Unknown backup strategy: " + backupStrategy);
    }
  }

  public static BackupStrategy toBackupStrategy(BackupStrategyGrpc backupStrategyGrpc) {
    switch (backupStrategyGrpc) {
      case KEEP_MIN_VERSIONS:
        return BackupStrategy.KEEP_MIN_VERSIONS;
      case DELETE_ON_NEW_PUSH_START:
        return BackupStrategy.DELETE_ON_NEW_PUSH_START;
      default:
        throw new IllegalArgumentException("Unknown backup strategy: " + backupStrategyGrpc);
    }
  }

  static PartitionerConfigGrpc toGrpcPartitionerConfig(PartitionerConfig partitionerConfig) {
    PartitionerConfigGrpc.Builder builder =
        PartitionerConfigGrpc.newBuilder().setPartitionerClass(partitionerConfig.getPartitionerClass());
    partitionerConfig.getPartitionerParams().forEach(builder::putPartitionerParams);
    return builder.build();
  }

  public static PartitionerConfig toPartitionerConfig(PartitionerConfigGrpc partitionerConfigGrpc) {
    Map<String, String> partitionerParams = new HashMap<>(partitionerConfigGrpc.getPartitionerParamsMap());
    return new PartitionerConfigImpl(partitionerConfigGrpc.getPartitionerClass(), partitionerParams, 1);
  }

  static HybridStoreConfigGrpc toGrpcHybridStoreConfig(HybridStoreConfig hybridStoreConfig) {
    HybridStoreConfigGrpc.Builder builder = HybridStoreConfigGrpc.newBuilder()
        .setRewindTimeInSeconds(hybridStoreConfig.getRewindTimeInSeconds())
        .setOffsetLagThresholdToGoOnline(hybridStoreConfig.getOffsetLagThresholdToGoOnline())
        .setProducerTimestampLagThresholdToGoOnlineInSeconds(
            hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds())
        .setDataReplicationPolicy(fromDataReplicationPolicy(hybridStoreConfig.getDataReplicationPolicy()))
        .setBufferReplayPolicy(fromBufferReplayPolicy(hybridStoreConfig.getBufferReplayPolicy()));
    return builder.build();
  }

  public static HybridStoreConfig toHybridStoreConfig(HybridStoreConfigGrpc hybridStoreConfigGrpc) {
    return new HybridStoreConfigImpl(
        hybridStoreConfigGrpc.getRewindTimeInSeconds(),
        hybridStoreConfigGrpc.getOffsetLagThresholdToGoOnline(),
        hybridStoreConfigGrpc.getProducerTimestampLagThresholdToGoOnlineInSeconds(),
        toDataReplicationPolicy(hybridStoreConfigGrpc.getDataReplicationPolicy()),
        toBufferReplayPolicy(hybridStoreConfigGrpc.getBufferReplayPolicy()));
  }

  static DataRecoveryVersionConfigGrpc toGrpcDataRecoveryVersionConfig(
      DataRecoveryVersionConfig dataRecoveryVersionConfig) {
    DataRecoveryVersionConfigGrpc.Builder builder = DataRecoveryVersionConfigGrpc.newBuilder()
        .setDataRecoverySourceFabric(dataRecoveryVersionConfig.getDataRecoverySourceFabric())
        .setDataRecoverySourceVersionNumber(dataRecoveryVersionConfig.getDataRecoverySourceVersionNumber())
        .setDataRecoveryComplete(dataRecoveryVersionConfig.isDataRecoveryComplete());
    return builder.build();
  }

  public static DataRecoveryVersionConfig toDataRecoveryVersionConfig(DataRecoveryVersionConfigGrpc configGrpc) {
    return new DataRecoveryVersionConfigImpl(
        configGrpc.getDataRecoverySourceFabric(),
        configGrpc.getDataRecoveryComplete(),
        configGrpc.getDataRecoverySourceVersionNumber());
  }

  static ViewConfigGrpc toGrpcViewConfig(ViewConfig viewConfig) {
    ViewConfigGrpc.Builder builder = ViewConfigGrpc.newBuilder().setViewClassName(viewConfig.getViewClassName());
    viewConfig.getViewParameters().forEach(builder::putViewParameters);
    return builder.build();
  }

  public static ViewConfig toViewConfig(ViewConfigGrpc viewConfigGrpc) {
    Map<String, String> viewParams = new HashMap<>(viewConfigGrpc.getViewParametersMap());
    return new ViewConfigImpl(viewConfigGrpc.getViewClassName(), viewParams);
  }

  // Map<String, ViewConfig> viewConfigs to map<string, ViewConfigGrpc> viewConfigs
  static Map<String, ViewConfigGrpc> toGrpcViewConfigs(Map<String, ViewConfig> viewConfigs) {
    Map<String, ViewConfigGrpc> viewConfigGrpcMap = new HashMap<>(viewConfigs.size());
    viewConfigs.forEach((key, value) -> viewConfigGrpcMap.put(key, toGrpcViewConfig(value)));
    return viewConfigGrpcMap;
  }

  public static Map<String, ViewConfig> toViewConfigs(Map<String, ViewConfigGrpc> viewConfigGrpcMap) {
    Map<String, ViewConfig> viewConfigs = new HashMap<>(viewConfigGrpcMap.size());
    viewConfigGrpcMap.forEach((key, value) -> viewConfigs.put(key, toViewConfig(value)));
    return viewConfigs;
  }

  public static ETLStoreConfigGrpc fromETLStoreConfigG(ETLStoreConfig etlStoreConfig) {
    return ETLStoreConfigGrpc.newBuilder()
        .setEtledUserProxyAccount(etlStoreConfig.getEtledUserProxyAccount())
        .setRegularVersionETLEnabled(etlStoreConfig.isRegularVersionETLEnabled())
        .setFutureVersionETLEnabled(etlStoreConfig.isFutureVersionETLEnabled())
        .build();
  }

  public static ETLStoreConfig toETLStoreConfig(ETLStoreConfigGrpc etlStoreConfigGrpc) {
    return new ETLStoreConfigImpl(
        etlStoreConfigGrpc.getEtledUserProxyAccount(),
        etlStoreConfigGrpc.getRegularVersionETLEnabled(),
        etlStoreConfigGrpc.getFutureVersionETLEnabled());
  }

  public static VersionGrpc toGrpcVersion(Version version) {
    VersionGrpc.Builder versionBuilder = VersionGrpc.newBuilder();
    versionBuilder.setStoreName(version.getStoreName())
        .setNumber(version.getNumber())
        .setCreatedTime(version.getCreatedTime())
        .setStatus(fromVersionStatus(version.getStatus()))
        .setPushJobId(version.getPushJobId())
        .setCompressionStrategy(fromCompressionStrategy(version.getCompressionStrategy()))
        .setPushStreamSourceAddress(version.getPushStreamSourceAddress())
        .setChunkingEnabled(version.isChunkingEnabled())
        .setRmdChunkingEnabled(version.isRmdChunkingEnabled())
        .setPushType(fromPushType(version.getPushType()))
        .setPartitionCount(version.getPartitionCount())
        .setPartitionerConfig(toGrpcPartitionerConfig(version.getPartitionerConfig()))
        .setReplicationFactor(version.getReplicationFactor())
        .setNativeReplicationSourceFabric(version.getNativeReplicationSourceFabric())
        .setIncrementalPushEnabled(version.isIncrementalPushEnabled())
        .setSeparateRealTimeTopicEnabled(version.isSeparateRealTimeTopicEnabled())
        .setBlobTransferEnabled(version.isBlobTransferEnabled())
        .setUseVersionLevelIncrementalPushEnabled(version.isUseVersionLevelIncrementalPushEnabled())
        .setUseVersionLevelHybridConfig(version.isUseVersionLevelHybridConfig())
        .setActiveActiveReplicationEnabled(version.isActiveActiveReplicationEnabled())
        .setDeferVersionSwap(version.isVersionSwapDeferred())
        .setRepushSourceVersion(version.getRepushSourceVersion());

    if (version.getDataRecoveryVersionConfig() != null) {
      versionBuilder.setDataRecoveryConfig(toGrpcDataRecoveryVersionConfig(version.getDataRecoveryVersionConfig()));
    }

    if (version.getHybridStoreConfig() != null) {
      versionBuilder.setHybridConfig(toGrpcHybridStoreConfig(version.getHybridStoreConfig()));
    }
    if (version.getViewConfigs() != null) {
      version.getViewConfigs().forEach((key, value) -> versionBuilder.putViewConfigs(key, toGrpcViewConfig(value)));
    }

    return versionBuilder.build();
  }

  public static Version toVersion(VersionGrpc versionGrpc) {
    Version version = new VersionImpl(
        versionGrpc.getStoreName(),
        versionGrpc.getNumber(),
        versionGrpc.getCreatedTime(),
        versionGrpc.getPushJobId(),
        versionGrpc.getPartitionCount(),
        toPartitionerConfig(versionGrpc.getPartitionerConfig()),
        toDataRecoveryVersionConfig(versionGrpc.getDataRecoveryConfig()));
    version.setStatus(toVersionStatus(versionGrpc.getStatus()));
    version.setCompressionStrategy(toCompressionStrategy(versionGrpc.getCompressionStrategy()));
    version.setPushStreamSourceAddress(versionGrpc.getPushStreamSourceAddress());
    version.setChunkingEnabled(versionGrpc.getChunkingEnabled());
    version.setRmdChunkingEnabled(versionGrpc.getRmdChunkingEnabled());
    version.setPushType(toPushType(versionGrpc.getPushType()));
    version.setReplicationFactor(versionGrpc.getReplicationFactor());
    version.setNativeReplicationSourceFabric(versionGrpc.getNativeReplicationSourceFabric());
    version.setIncrementalPushEnabled(versionGrpc.getIncrementalPushEnabled());
    version.setSeparateRealTimeTopicEnabled(versionGrpc.getSeparateRealTimeTopicEnabled());
    version.setBlobTransferEnabled(versionGrpc.getBlobTransferEnabled());
    version.setUseVersionLevelIncrementalPushEnabled(versionGrpc.getUseVersionLevelIncrementalPushEnabled());
    version.setUseVersionLevelHybridConfig(versionGrpc.getUseVersionLevelHybridConfig());
    version.setActiveActiveReplicationEnabled(versionGrpc.getActiveActiveReplicationEnabled());
    version.setVersionSwapDeferred(versionGrpc.getDeferVersionSwap());
    version.setViewConfigs(toViewConfigs(versionGrpc.getViewConfigsMap()));
    version.setRepushSourceVersion(versionGrpc.getRepushSourceVersion());

    if (versionGrpc.hasDataRecoveryConfig()) {
      version.setDataRecoveryVersionConfig(toDataRecoveryVersionConfig(versionGrpc.getDataRecoveryConfig()));
    }
    if (versionGrpc.hasHybridConfig()) {
      version.setHybridStoreConfig(toHybridStoreConfig(versionGrpc.getHybridConfig()));
    }

    return version;
  }

  public static StoreInfoGrpc toStoreInfoGrpc(StoreInfo storeInfo) {
    StoreInfoGrpc.Builder builder = StoreInfoGrpc.newBuilder()
        .setName(storeInfo.getName())
        .setOwner(storeInfo.getOwner())
        .setPartitionCount(storeInfo.getPartitionCount())
        .setCurrentVersion(storeInfo.getCurrentVersion())
        .setReservedVersion(storeInfo.getReservedVersion())
        .setLowWatermark(storeInfo.getLowWatermark())
        .setEnableStoreWrites(storeInfo.isEnableStoreWrites())
        .setEnableStoreReads(storeInfo.isEnableStoreReads())
        .setStorageQuotaInByte(storeInfo.getStorageQuotaInByte())
        .setHybridStoreOverheadBypass(storeInfo.getHybridStoreOverheadBypass())
        .setReadQuotaInCU(storeInfo.getReadQuotaInCU())
        .setAccessControlled(storeInfo.isAccessControlled())
        .setChunkingEnabled(storeInfo.isChunkingEnabled())
        .setRmdChunkingEnabled(storeInfo.isRmdChunkingEnabled())
        .setSingleGetRouterCacheEnabled(storeInfo.isSingleGetRouterCacheEnabled())
        .setBatchGetRouterCacheEnabled(storeInfo.isBatchGetRouterCacheEnabled())
        .setBatchGetLimit(storeInfo.getBatchGetLimit())
        .setLargestUsedVersionNumber(storeInfo.getLargestUsedVersionNumber())
        .setIncrementalPushEnabled(storeInfo.isIncrementalPushEnabled())
        .setClientDecompressionEnabled(storeInfo.getClientDecompressionEnabled())
        .setNumVersionsToPreserve(storeInfo.getNumVersionsToPreserve())
        .setMigrating(storeInfo.isMigrating())
        .setWriteComputationEnabled(storeInfo.isWriteComputationEnabled())
        .setReplicationMetadataVersionId(storeInfo.getReplicationMetadataVersionId())
        .setReadComputationEnabled(storeInfo.isReadComputationEnabled())
        .setBootstrapToOnlineTimeoutInHours(storeInfo.getBootstrapToOnlineTimeoutInHours())
        .setNativeReplicationEnabled(storeInfo.isNativeReplicationEnabled())
        .setPushStreamSourceAddress(storeInfo.getPushStreamSourceAddress())
        .setSchemaAutoRegisterFromPushJobEnabled(storeInfo.isSchemaAutoRegisterFromPushJobEnabled())
        .setSuperSetSchemaAutoGenerationForReadComputeEnabled(
            storeInfo.isSuperSetSchemaAutoGenerationForReadComputeEnabled())
        .setLatestSuperSetValueSchemaId(storeInfo.getLatestSuperSetValueSchemaId())
        .setHybridStoreDiskQuotaEnabled(storeInfo.isHybridStoreDiskQuotaEnabled())
        .setBackupVersionRetentionMs(storeInfo.getBackupVersionRetentionMs())
        .setReplicationFactor(storeInfo.getReplicationFactor())
        .setMigrationDuplicateStore(storeInfo.isMigrationDuplicateStore())
        .setNativeReplicationSourceFabric(storeInfo.getNativeReplicationSourceFabric())
        .setStoreMetadataSystemStoreEnabled(storeInfo.isStoreMetadataSystemStoreEnabled())
        .setStoreMetaSystemStoreEnabled(storeInfo.isStoreMetaSystemStoreEnabled())
        .setDaVinciPushStatusStoreEnabled(storeInfo.isDaVinciPushStatusStoreEnabled())
        .setActiveActiveReplicationEnabled(storeInfo.isActiveActiveReplicationEnabled())
        .setKafkaBrokerUrl(storeInfo.getKafkaBrokerUrl())
        .setStorageNodeReadQuotaEnabled(storeInfo.isStorageNodeReadQuotaEnabled())
        .setMinCompactionLagSeconds(storeInfo.getMinCompactionLagSeconds())
        .setMaxCompactionLagSeconds(storeInfo.getMaxCompactionLagSeconds())
        .setMaxRecordSizeBytes(storeInfo.getMaxRecordSizeBytes())
        .setMaxNearlineRecordSizeBytes(storeInfo.getMaxNearlineRecordSizeBytes())
        .setUnusedSchemaDeletionEnabled(storeInfo.isUnusedSchemaDeletionEnabled())
        .setBlobTransferEnabled(storeInfo.isBlobTransferEnabled());

    if (storeInfo.getColoToCurrentVersions() != null) {
      storeInfo.getColoToCurrentVersions().forEach(builder::putColoToCurrentVersions);
    }
    for (Version version: storeInfo.getVersions()) {
      builder.addVersions(toGrpcVersion(version));
    }
    if (storeInfo.getHybridStoreConfig() != null) {
      builder.setHybridStoreConfig(toGrpcHybridStoreConfig(storeInfo.getHybridStoreConfig()));
    }

    builder.setCompressionStrategy(fromCompressionStrategy(storeInfo.getCompressionStrategy()))
        .setBackupStrategy(fromBackupStrategy(storeInfo.getBackupStrategy()))
        .setEtlStoreConfig(fromETLStoreConfigG(storeInfo.getEtlStoreConfig()))
        .setPartitionerConfig(toGrpcPartitionerConfig(storeInfo.getPartitionerConfig()));
    for (Map.Entry<String, ViewConfig> entry: storeInfo.getViewConfigs().entrySet()) {
      builder.putViewConfigs(entry.getKey(), toGrpcViewConfig(entry.getValue()));
    }
    return builder.build();
  }

  public static StoreInfo toStoreInfo(StoreInfoGrpc storeInfoGrpc) {
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setName(storeInfoGrpc.getName());
    storeInfo.setOwner(storeInfoGrpc.getOwner());
    storeInfo.setPartitionCount(storeInfoGrpc.getPartitionCount());
    storeInfo.setCurrentVersion(storeInfoGrpc.getCurrentVersion());
    storeInfo.setReservedVersion(storeInfoGrpc.getReservedVersion());
    storeInfo.setLowWatermark(storeInfoGrpc.getLowWatermark());
    storeInfo.setEnableStoreWrites(storeInfoGrpc.getEnableStoreWrites());
    storeInfo.setEnableStoreReads(storeInfoGrpc.getEnableStoreReads());
    storeInfo.setStorageQuotaInByte(storeInfoGrpc.getStorageQuotaInByte());
    storeInfo.setHybridStoreOverheadBypass(storeInfoGrpc.getHybridStoreOverheadBypass());
    storeInfo.setReadQuotaInCU(storeInfoGrpc.getReadQuotaInCU());
    storeInfo.setAccessControlled(storeInfoGrpc.getAccessControlled());
    storeInfo.setChunkingEnabled(storeInfoGrpc.getChunkingEnabled());
    storeInfo.setRmdChunkingEnabled(storeInfoGrpc.getRmdChunkingEnabled());
    storeInfo.setSingleGetRouterCacheEnabled(storeInfoGrpc.getSingleGetRouterCacheEnabled());
    storeInfo.setBatchGetRouterCacheEnabled(storeInfoGrpc.getBatchGetRouterCacheEnabled());
    storeInfo.setBatchGetLimit(storeInfoGrpc.getBatchGetLimit());
    storeInfo.setLargestUsedVersionNumber(storeInfoGrpc.getLargestUsedVersionNumber());
    storeInfo.setIncrementalPushEnabled(storeInfoGrpc.getIncrementalPushEnabled());
    storeInfo.setClientDecompressionEnabled(storeInfoGrpc.getClientDecompressionEnabled());
    storeInfo.setNumVersionsToPreserve(storeInfoGrpc.getNumVersionsToPreserve());
    storeInfo.setMigrating(storeInfoGrpc.getMigrating());
    storeInfo.setWriteComputationEnabled(storeInfoGrpc.getWriteComputationEnabled());
    storeInfo.setReplicationMetadataVersionId(storeInfoGrpc.getReplicationMetadataVersionId());
    storeInfo.setReadComputationEnabled(storeInfoGrpc.getReadComputationEnabled());
    storeInfo.setBootstrapToOnlineTimeoutInHours(storeInfoGrpc.getBootstrapToOnlineTimeoutInHours());
    storeInfo.setNativeReplicationEnabled(storeInfoGrpc.getNativeReplicationEnabled());
    storeInfo.setPushStreamSourceAddress(storeInfoGrpc.getPushStreamSourceAddress());
    storeInfo.setSchemaAutoRegisterFromPushJobEnabled(storeInfoGrpc.getSchemaAutoRegisterFromPushJobEnabled());
    storeInfo.setSuperSetSchemaAutoGenerationForReadComputeEnabled(
        storeInfoGrpc.getSuperSetSchemaAutoGenerationForReadComputeEnabled());
    storeInfo.setLatestSuperSetValueSchemaId(storeInfoGrpc.getLatestSuperSetValueSchemaId());
    storeInfo.setHybridStoreDiskQuotaEnabled(storeInfoGrpc.getHybridStoreDiskQuotaEnabled());
    storeInfo.setBackupVersionRetentionMs(storeInfoGrpc.getBackupVersionRetentionMs());
    storeInfo.setReplicationFactor(storeInfoGrpc.getReplicationFactor());
    storeInfo.setMigrationDuplicateStore(storeInfoGrpc.getMigrationDuplicateStore());
    storeInfo.setNativeReplicationSourceFabric(storeInfoGrpc.getNativeReplicationSourceFabric());
    storeInfo.setStoreMetadataSystemStoreEnabled(storeInfoGrpc.getStoreMetadataSystemStoreEnabled());
    storeInfo.setStoreMetaSystemStoreEnabled(storeInfoGrpc.getStoreMetaSystemStoreEnabled());
    storeInfo.setDaVinciPushStatusStoreEnabled(storeInfoGrpc.getDaVinciPushStatusStoreEnabled());
    storeInfo.setActiveActiveReplicationEnabled(storeInfoGrpc.getActiveActiveReplicationEnabled());
    storeInfo.setKafkaBrokerUrl(storeInfoGrpc.getKafkaBrokerUrl());
    storeInfo.setStorageNodeReadQuotaEnabled(storeInfoGrpc.getStorageNodeReadQuotaEnabled());
    storeInfo.setMinCompactionLagSeconds(storeInfoGrpc.getMinCompactionLagSeconds());
    storeInfo.setMaxCompactionLagSeconds(storeInfoGrpc.getMaxCompactionLagSeconds());
    storeInfo.setMaxRecordSizeBytes(storeInfoGrpc.getMaxRecordSizeBytes());
    storeInfo.setMaxNearlineRecordSizeBytes(storeInfoGrpc.getMaxNearlineRecordSizeBytes());
    storeInfo.setUnusedSchemaDeletionEnabled(storeInfoGrpc.getUnusedSchemaDeletionEnabled());
    storeInfo.setBlobTransferEnabled(storeInfoGrpc.getBlobTransferEnabled());

    List<Version> versions = new ArrayList<>(storeInfoGrpc.getVersionsList().size());
    for (VersionGrpc versionGrpc: storeInfoGrpc.getVersionsList()) {
      versions.add(toVersion(versionGrpc));
    }
    storeInfo.setVersions(versions);
    // Set view configs
    storeInfo.setViewConfigs(toViewConfigs(storeInfoGrpc.getViewConfigsMap()));

    // Set other configurations
    if (storeInfoGrpc.hasHybridStoreConfig()) {
      storeInfo.setHybridStoreConfig(toHybridStoreConfig(storeInfoGrpc.getHybridStoreConfig()));
    }
    if (storeInfoGrpc.hasEtlStoreConfig()) {
      storeInfo.setEtlStoreConfig(toETLStoreConfig(storeInfoGrpc.getEtlStoreConfig()));
    }
    if (storeInfoGrpc.hasPartitionerConfig()) {
      storeInfo.setPartitionerConfig(toPartitionerConfig(storeInfoGrpc.getPartitionerConfig()));
    }

    storeInfo.setCompressionStrategy(toCompressionStrategy(storeInfoGrpc.getCompressionStrategy()));
    storeInfo.setBackupStrategy(toBackupStrategy(storeInfoGrpc.getBackupStrategy()));
    return storeInfo;
  }

  public static EmptyPushGrpcResponse fromEmptyPushGrpcResponse(VersionCreationResponse response) {
    EmptyPushGrpcResponse.Builder builder = EmptyPushGrpcResponse.newBuilder()
        .setClusterStoreInfo(GrpcRequestResponseConverter.getClusterStoreGrpcInfo(response))
        .setVersion(response.getVersion())
        .setPartitions(response.getPartitions())
        .setReplicas(response.getReplicas())
        .setEnableSSL(response.isEnableSSL())
        .setCompressionStrategy(GrpcRequestResponseConverter.fromCompressionStrategy(response.getCompressionStrategy()))
        .setDaVinciPushStatusStoreEnabled(response.isDaVinciPushStatusStoreEnabled());
    if (response.getPartitionerClass() != null) {
      builder.setPartitionerClass(response.getPartitionerClass());
    }
    if (response.getKafkaTopic() != null) {
      builder.setPubSubTopic(response.getKafkaTopic());
    }
    if (response.getKafkaBootstrapServers() != null) {
      builder.setPubSubBootstrapServers(response.getKafkaBootstrapServers());
    }
    if (response.getKafkaSourceRegion() != null) {
      builder.setPubSubSourceRegion(response.getKafkaSourceRegion());
    }
    Map<String, String> partitionerParams = response.getPartitionerParams();
    if (partitionerParams != null) {
      builder.putAllPartitionerParams(partitionerParams);
    }
    return builder.build();
  }

  public static VersionCreationResponse toVersionCreationResponse(EmptyPushGrpcResponse grpcResponse) {
    VersionCreationResponse response = new VersionCreationResponse();
    response.setCluster(grpcResponse.getClusterStoreInfo().getClusterName());
    response.setName(grpcResponse.getClusterStoreInfo().getStoreName());
    response.setVersion(grpcResponse.getVersion());
    response.setPartitions(grpcResponse.getPartitions());
    response.setReplicas(grpcResponse.getReplicas());
    response.setKafkaTopic(grpcResponse.getPubSubTopic());
    response.setKafkaBootstrapServers(grpcResponse.getPubSubBootstrapServers());
    response.setEnableSSL(grpcResponse.getEnableSSL());
    response.setCompressionStrategy(
        GrpcRequestResponseConverter.toCompressionStrategy(grpcResponse.getCompressionStrategy()));
    response.setPartitionerClass(grpcResponse.getPartitionerClass());
    response.setDaVinciPushStatusStoreEnabled(grpcResponse.getDaVinciPushStatusStoreEnabled());
    response.setKafkaSourceRegion(grpcResponse.getPubSubSourceRegion());
    response.setPartitionerParams(grpcResponse.getPartitionerParamsMap());
    return response;
  }
}
