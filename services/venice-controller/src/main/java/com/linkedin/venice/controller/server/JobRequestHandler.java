package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.meta.UncompletedReplica;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.UncompletedPartitionGrpc;
import com.linkedin.venice.protocols.controller.UncompletedReplicaGrpc;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Request handler for job-related operations in the Venice controller.
 */
public class JobRequestHandler {
  private static final Logger LOGGER = LogManager.getLogger(JobRequestHandler.class);

  private final Admin admin;

  public JobRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
  }

  /**
   * Gets the status of an offline push job for a specific store version.
   * @param request the request containing cluster, store, version, and optional parameters
   * @return response containing job status details
   */
  public GetJobStatusGrpcResponse getJobStatus(GetJobStatusGrpcRequest request) {
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    ControllerRequestParamValidator.validateClusterStoreInfo(storeInfo);
    String clusterName = storeInfo.getClusterName();
    String storeName = storeInfo.getStoreName();
    int versionNumber = request.getVersion();

    if (versionNumber <= 0) {
      throw new IllegalArgumentException("Version number must be a positive integer");
    }

    Optional<String> incrementalPushVersion =
        request.hasIncrementalPushVersion() ? Optional.of(request.getIncrementalPushVersion()) : Optional.empty();
    String targetedRegions = request.hasTargetedRegions() ? request.getTargetedRegions() : null;
    boolean isTargetRegionPushWithDeferredSwap =
        request.hasIsTargetRegionPushWithDeferredSwap() && request.getIsTargetRegionPushWithDeferredSwap();
    String region = request.hasRegion() ? request.getRegion() : null;

    LOGGER.info("Getting job status for store: {} version: {} in cluster: {}", storeName, versionNumber, clusterName);

    String kafkaTopicName = Version.composeKafkaTopic(storeName, versionNumber);

    Admin.OfflinePushStatusInfo offlineJobStatus = admin.getOffLinePushStatus(
        clusterName,
        kafkaTopicName,
        incrementalPushVersion,
        region,
        targetedRegions,
        isTargetRegionPushWithDeferredSwap);

    GetJobStatusGrpcResponse.Builder responseBuilder = GetJobStatusGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .setVersion(versionNumber)
        .setStatus(offlineJobStatus.getExecutionStatus().toString());

    if (offlineJobStatus.getStatusDetails() != null) {
      responseBuilder.setStatusDetails(offlineJobStatus.getStatusDetails());
    }

    if (offlineJobStatus.getStatusUpdateTimestamp() != null) {
      responseBuilder.setStatusUpdateTimestamp(offlineJobStatus.getStatusUpdateTimestamp());
    }

    Map<String, String> extraInfo = offlineJobStatus.getExtraInfo();
    if (extraInfo != null) {
      responseBuilder.putAllExtraInfo(extraInfo);
    }

    Map<String, String> extraDetails = offlineJobStatus.getExtraDetails();
    if (extraDetails != null) {
      responseBuilder.putAllExtraDetails(extraDetails);
    }

    Map<String, Long> extraInfoUpdateTimestamp = offlineJobStatus.getExtraInfoUpdateTimestamp();
    if (extraInfoUpdateTimestamp != null) {
      responseBuilder.putAllExtraInfoUpdateTimestamp(extraInfoUpdateTimestamp);
    }

    List<UncompletedPartition> uncompletedPartitions = offlineJobStatus.getUncompletedPartitions();
    if (uncompletedPartitions != null) {
      for (UncompletedPartition partition: uncompletedPartitions) {
        UncompletedPartitionGrpc.Builder partitionBuilder =
            UncompletedPartitionGrpc.newBuilder().setPartitionId(partition.getPartitionId());

        List<UncompletedReplica> replicas = partition.getUncompletedReplicas();
        if (replicas != null) {
          for (UncompletedReplica replica: replicas) {
            UncompletedReplicaGrpc.Builder replicaBuilder = UncompletedReplicaGrpc.newBuilder()
                .setInstanceId(replica.getInstanceId())
                .setStatus(replica.getStatus().toString())
                .setCurrentOffset(replica.getCurrentOffset());

            if (replica.getStatusDetails() != null) {
              replicaBuilder.setStatusDetails(replica.getStatusDetails());
            }

            partitionBuilder.addUncompletedReplicas(replicaBuilder.build());
          }
        }

        responseBuilder.addUncompletedPartitions(partitionBuilder.build());
      }
    }

    return responseBuilder.build();
  }
}
