package com.linkedin.venice.controllerapi.transport;

import com.google.rpc.ErrorInfo;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.controllerapi.request.UpdateAclForStoreRequest;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataRecoveryVersionConfig;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.protocols.BackupStrategyGrpc;
import com.linkedin.venice.protocols.BufferReplayPolicyGrpc;
import com.linkedin.venice.protocols.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.CompressionStrategyGrpc;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.DataRecoveryVersionConfigGrpc;
import com.linkedin.venice.protocols.DataReplicationPolicyGrpc;
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

  public StoreInfoGrpc toGrpcStoreInfo(StoreInfo storeInfo) {
    StoreInfoGrpc.Builder builder = StoreInfoGrpc.newBuilder()
        .setName(storeInfo.getName())
        .setOwner(storeInfo.getOwner())
        .setCurrentVersion(storeInfo.getCurrentVersion())
        .setReservedVersion(storeInfo.getReservedVersion())
        .setPartitionCount(storeInfo.getPartitionCount())
        .setLowWatermark(storeInfo.getLowWatermark())
        .setEnableStoreWrites(storeInfo.isEnableStoreWrites())
        .setEnableStoreReads(storeInfo.isEnableStoreReads());

    storeInfo.getColoToCurrentVersions().forEach(builder::putColoToCurrentVersions);
    return builder.build();
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

  static PartitionerConfigGrpc toGrpcPartitionerConfig(PartitionerConfig partitionerConfig) {
    PartitionerConfigGrpc.Builder builder =
        PartitionerConfigGrpc.newBuilder().setPartitionerClass(partitionerConfig.getPartitionerClass());
    partitionerConfig.getPartitionerParams().forEach(builder::putPartitionerParams);
    return builder.build();
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

  static DataRecoveryVersionConfigGrpc toGrpcDataRecoveryVersionConfig(
      DataRecoveryVersionConfig dataRecoveryVersionConfig) {
    DataRecoveryVersionConfigGrpc.Builder builder = DataRecoveryVersionConfigGrpc.newBuilder()
        .setDataRecoverySourceFabric(dataRecoveryVersionConfig.getDataRecoverySourceFabric())
        .setDataRecoverySourceVersionNumber(dataRecoveryVersionConfig.getDataRecoverySourceVersionNumber())
        .setDataRecoveryComplete(dataRecoveryVersionConfig.isDataRecoveryComplete());
    return builder.build();
  }

  static ViewConfigGrpc toGrpcViewConfig(ViewConfig viewConfig) {
    ViewConfigGrpc.Builder builder = ViewConfigGrpc.newBuilder().setViewClassName(viewConfig.getViewClassName());
    viewConfig.getViewParameters().forEach(builder::putViewParameters);
    return builder.build();
  }

  // Map<String, ViewConfig> viewConfigs to map<string, ViewConfigGrpc> viewConfigs
  static Map<String, ViewConfigGrpc> toGrpcViewConfigs(Map<String, ViewConfig> viewConfigs) {
    Map<String, ViewConfigGrpc> viewConfigGrpcMap = new HashMap<>(viewConfigs.size());
    viewConfigs.forEach((key, value) -> viewConfigGrpcMap.put(key, toGrpcViewConfig(value)));
    return viewConfigGrpcMap;
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
        .setHybridConfig(toGrpcHybridStoreConfig(version.getHybridStoreConfig()))
        .setUseVersionLevelHybridConfig(version.isUseVersionLevelHybridConfig())
        .setActiveActiveReplicationEnabled(version.isActiveActiveReplicationEnabled())
        .setDataRecoveryConfig(toGrpcDataRecoveryVersionConfig(version.getDataRecoveryVersionConfig()))
        .setDeferVersionSwap(version.isVersionSwapDeferred())
        .putAllViewConfigs(toGrpcViewConfigs(version.getViewConfigs()))
        .setRepushSourceVersion(version.getRepushSourceVersion());
    return versionBuilder.build();
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

  public static StoreInfoGrpc toStoreInfoGrpc(StoreInfo storeInfo) {
    StoreInfoGrpc.Builder builder = StoreInfoGrpc.newBuilder()
        .setName(storeInfo.getName())
        .setOwner(storeInfo.getOwner())
        .setCurrentVersion(storeInfo.getCurrentVersion())
        .setReservedVersion(storeInfo.getReservedVersion())
        .setPartitionCount(storeInfo.getPartitionCount())
        .setLowWatermark(storeInfo.getLowWatermark())
        .setEnableStoreWrites(storeInfo.isEnableStoreWrites())
        .setEnableStoreReads(storeInfo.isEnableStoreReads())
        .setStorageQuotaInByte(storeInfo.getStorageQuotaInByte())
        .setHybridStoreOverheadBypass(storeInfo.getHybridStoreOverheadBypass())
        .setReadQuotaInCU(storeInfo.getReadQuotaInCU())
        .setHybridStoreConfig(toGrpcHybridStoreConfig(storeInfo.getHybridStoreConfig()))
        .setAccessControlled(storeInfo.isAccessControlled())
        .setChunkingEnabled(storeInfo.isChunkingEnabled())
        .setRmdChunkingEnabled(storeInfo.isRmdChunkingEnabled())
        .setSingleGetRouterCacheEnabled(storeInfo.isSingleGetRouterCacheEnabled())
        .setBatchGetRouterCacheEnabled(storeInfo.isBatchGetRouterCacheEnabled())
        .setBatchGetLimit(storeInfo.getBatchGetLimit())
        .setLargestUsedVersionNumber(storeInfo.getLargestUsedVersionNumber())
        .setIncrementalPushEnabled(storeInfo.isIncrementalPushEnabled())
        .setCompressionStrategy(fromCompressionStrategy(storeInfo.getCompressionStrategy()))
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
        .setBackupStrategy(fromBackupStrategy(storeInfo.getBackupStrategy()))
        .setBackupVersionRetentionMs(storeInfo.getBackupVersionRetentionMs())
        .setReplicationFactor(storeInfo.getReplicationFactor())
        .setMigrationDuplicateStore(storeInfo.isMigrationDuplicateStore())
        .setNativeReplicationSourceFabric(storeInfo.getNativeReplicationSourceFabric())
        .setStoreMetadataSystemStoreEnabled(storeInfo.isStoreMetadataSystemStoreEnabled())
        .setStoreMetaSystemStoreEnabled(storeInfo.isStoreMetaSystemStoreEnabled())
        .setDaVinciPushStatusStoreEnabled(storeInfo.isDaVinciPushStatusStoreEnabled())
        .setActiveActiveReplicationEnabled(storeInfo.isActiveActiveReplicationEnabled())
        .setKafkaBrokerUrl(storeInfo.getKafkaBrokerUrl())
        .putAllViewConfigs(toGrpcViewConfigs(storeInfo.getViewConfigs()))
        .setStorageNodeReadQuotaEnabled(storeInfo.isStorageNodeReadQuotaEnabled())
        .setMinCompactionLagSeconds(storeInfo.getMinCompactionLagSeconds())
        .setMaxCompactionLagSeconds(storeInfo.getMaxCompactionLagSeconds())
        .setMaxRecordSizeBytes(storeInfo.getMaxRecordSizeBytes())
        .setMaxNearlineRecordSizeBytes(storeInfo.getMaxNearlineRecordSizeBytes())
        .setUnusedSchemaDeletionEnabled(storeInfo.isUnusedSchemaDeletionEnabled())
        .setBlobTransferEnabled(storeInfo.isBlobTransferEnabled());

    storeInfo.getColoToCurrentVersions().forEach(builder::putColoToCurrentVersions);

    List<Version> versions = storeInfo.getVersions();
    for (Version version: versions) {
      builder.addVersions(toGrpcVersion(version));
    }

    return builder.build();
  }
}
