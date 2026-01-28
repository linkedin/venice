package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCREMENTAL_PUSH_VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGETED_REGIONS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGET_REGION_PUSH_WITH_DEFERRED_SWAP;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TOPIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ONGOING_INCREMENTAL_PUSH_VERSIONS;
import static com.linkedin.venice.controllerapi.ControllerRoute.JOB;
import static com.linkedin.venice.controllerapi.ControllerRoute.KILL_OFFLINE_PUSH_JOB;
import static com.linkedin.venice.controllerapi.ControllerRoute.SEND_PUSH_JOB_DETAILS;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.IncrementalPushVersionsResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.routes.PushJobStatusUploadResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.meta.UncompletedReplica;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.UncompletedPartitionGrpc;
import com.linkedin.venice.protocols.controller.UncompletedReplicaGrpc;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Route;


public class JobRoutes extends AbstractRoute {
  private static final Logger LOGGER = LogManager.getLogger(JobRoutes.class);
  private final InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsSerializer =
      AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();
  private final JobRequestHandler jobRequestHandler;

  public JobRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      JobRequestHandler jobRequestHandler) {
    super(sslEnabled, accessController);
    this.jobRequestHandler = jobRequestHandler;
  }

  /**
   * @see Admin#getOffLinePushStatus(String, String)
   */
  public Route jobStatus(Admin admin) {
    return (request, response) -> {
      JobStatusQueryResponse responseObject = new JobStatusQueryResponse();
      response.type(HttpConstants.JSON);
      try {
        // No ACL check for getting job metadata
        AdminSparkServer.validateParams(request, JOB.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String store = request.queryParams(NAME);
        int versionNumber = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        String incrementalPushVersion = AdminSparkServer.getOptionalParameterValue(request, INCREMENTAL_PUSH_VERSION);
        String targetedRegions = request.queryParams(TARGETED_REGIONS);
        boolean isTargetRegionPushWithDeferredSwap =
            Boolean.parseBoolean(request.queryParams(TARGET_REGION_PUSH_WITH_DEFERRED_SWAP));
        String region = AdminSparkServer.getOptionalParameterValue(request, FABRIC);

        // Convert to gRPC request
        ClusterStoreGrpcInfo storeInfo =
            ClusterStoreGrpcInfo.newBuilder().setClusterName(cluster).setStoreName(store).build();
        GetJobStatusGrpcRequest.Builder grpcRequestBuilder =
            GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(versionNumber);
        if (incrementalPushVersion != null) {
          grpcRequestBuilder.setIncrementalPushVersion(incrementalPushVersion);
        }
        if (targetedRegions != null) {
          grpcRequestBuilder.setTargetedRegions(targetedRegions);
        }
        grpcRequestBuilder.setIsTargetRegionPushWithDeferredSwap(isTargetRegionPushWithDeferredSwap);
        if (region != null) {
          grpcRequestBuilder.setRegion(region);
        }

        // Call handler
        GetJobStatusGrpcResponse grpcResponse = jobRequestHandler.getJobStatus(grpcRequestBuilder.build());

        // Map response back to HTTP
        responseObject = mapGrpcResponseToHttp(grpcResponse);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * Maps the gRPC response to the HTTP response format.
   */
  JobStatusQueryResponse mapGrpcResponseToHttp(GetJobStatusGrpcResponse grpcResponse) {
    JobStatusQueryResponse responseObject = new JobStatusQueryResponse();
    responseObject.setCluster(grpcResponse.getStoreInfo().getClusterName());
    responseObject.setName(grpcResponse.getStoreInfo().getStoreName());
    responseObject.setVersion(grpcResponse.getVersion());
    responseObject.setStatus(grpcResponse.getStatus());

    if (grpcResponse.hasStatusDetails()) {
      responseObject.setStatusDetails(grpcResponse.getStatusDetails());
    }
    if (grpcResponse.hasStatusUpdateTimestamp()) {
      responseObject.setStatusUpdateTimestamp(grpcResponse.getStatusUpdateTimestamp());
    }

    Map<String, String> extraInfo = grpcResponse.getExtraInfoMap();
    if (!extraInfo.isEmpty()) {
      responseObject.setExtraInfo(extraInfo);
    }

    Map<String, String> extraDetails = grpcResponse.getExtraDetailsMap();
    if (!extraDetails.isEmpty()) {
      responseObject.setExtraDetails(extraDetails);
    }

    Map<String, Long> extraInfoUpdateTimestamp = grpcResponse.getExtraInfoUpdateTimestampMap();
    if (!extraInfoUpdateTimestamp.isEmpty()) {
      responseObject.setExtraInfoUpdateTimestamp(extraInfoUpdateTimestamp);
    }

    List<UncompletedPartitionGrpc> grpcPartitions = grpcResponse.getUncompletedPartitionsList();
    if (!grpcPartitions.isEmpty()) {
      List<UncompletedPartition> partitions = new ArrayList<>();
      for (UncompletedPartitionGrpc grpcPartition: grpcPartitions) {
        List<UncompletedReplica> replicas = new ArrayList<>();
        for (UncompletedReplicaGrpc grpcReplica: grpcPartition.getUncompletedReplicasList()) {
          UncompletedReplica replica = new UncompletedReplica(
              grpcReplica.getInstanceId(),
              ExecutionStatus.valueOf(grpcReplica.getStatus()),
              grpcReplica.getCurrentOffset(),
              grpcReplica.hasStatusDetails() ? grpcReplica.getStatusDetails() : null);
          replicas.add(replica);
        }
        partitions.add(new UncompletedPartition(grpcPartition.getPartitionId(), replicas));
      }
      responseObject.setUncompletedPartitions(partitions);
    }

    return responseObject;
  }

  /**
   * @see Admin#killOfflinePush(String, String, boolean)
   */
  public Route killOfflinePushJob(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow allowlist users to run this command
        if (!isAllowListUser(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject
              .setError("You don't have permission to kill this push job; please grant write ACL for yourself.");
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, KILL_OFFLINE_PUSH_JOB.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String topic = request.queryParams(TOPIC);
        responseObject.setCluster(cluster);
        responseObject.setName(Version.parseStoreFromKafkaTopicName(topic));

        admin.killOfflinePush(cluster, topic, false);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  public Route uploadPushJobStatus(Admin admin) {
    return (request, response) -> {
      PushJobStatusUploadResponse responseObject = new PushJobStatusUploadResponse();
      response.type(HttpConstants.JSON);
      // TODO: remove once all vpj plugin deployments have updated and no longer calling into this for reporting.
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#sendPushJobDetails(PushJobStatusRecordKey, PushJobDetails)
   */
  public Route sendPushJobDetails(Admin admin) {
    return ((request, response) -> {
      ControllerResponse controllerResponse = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // No ACL check for uploading push status
        AdminSparkServer.validateParams(request, SEND_PUSH_JOB_DETAILS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int versionNumber = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        controllerResponse.setCluster(clusterName);
        controllerResponse.setName(storeName);
        PushJobStatusRecordKey key = new PushJobStatusRecordKey();
        key.storeName = storeName;
        key.versionNumber = versionNumber;
        PushJobDetails pushJobDetails = pushJobDetailsSerializer.deserialize(null, request.bodyAsBytes());
        admin.sendPushJobDetails(key, pushJobDetails);

        if (pushJobDetails.sendLivenessHeartbeatFailureDetails != null) {
          LOGGER.warn(
              "Sending push job liveness heartbeats for store {} with version {} failed due to "
                  + "{}. Push job ID is: {}",
              storeName,
              versionNumber,
              pushJobDetails.failureDetails.toString(),
              pushJobDetails.pushId.toString());
        }

      } catch (Throwable e) {
        controllerResponse.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(controllerResponse);
    });
  }

  // TODO: remove the below API after the same version of codes is released to Venice Push Job.
  public Route getOngoingIncrementalPushVersions(Admin admin) {
    return ((request, response) -> {
      IncrementalPushVersionsResponse incrementalPushVersionsResponse = new IncrementalPushVersionsResponse();
      response.type(HttpConstants.JSON);
      try {
        AdminSparkServer.validateParams(request, GET_ONGOING_INCREMENTAL_PUSH_VERSIONS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String versionTopic = request.queryParams(TOPIC);
        incrementalPushVersionsResponse.setCluster(clusterName);
        incrementalPushVersionsResponse.setName(Version.parseStoreFromKafkaTopicName(versionTopic));
        incrementalPushVersionsResponse.setIncrementalPushVersions(Collections.emptySet());
      } catch (Throwable e) {
        incrementalPushVersionsResponse.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(incrementalPushVersionsResponse);
    });
  }
}
