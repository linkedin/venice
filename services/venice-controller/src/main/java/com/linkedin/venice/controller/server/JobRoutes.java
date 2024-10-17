package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCREMENTAL_PUSH_VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGETED_REGIONS;
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
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Route;


public class JobRoutes extends AbstractRoute {
  private static final Logger LOGGER = LogManager.getLogger(JobRoutes.class);
  private final InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsSerializer =
      AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();

  public JobRoutes(boolean sslEnabled, Optional<DynamicAccessController> accessController) {
    super(sslEnabled, accessController);
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
        String region = AdminSparkServer.getOptionalParameterValue(request, FABRIC);
        responseObject = populateJobStatus(
            cluster,
            store,
            versionNumber,
            admin,
            Optional.ofNullable(incrementalPushVersion),
            region,
            targetedRegions);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  JobStatusQueryResponse populateJobStatus(
      String cluster,
      String store,
      int versionNumber,
      Admin admin,
      Optional<String> incrementalPushVersion,
      String region,
      String targetedRegions) {
    JobStatusQueryResponse responseObject = new JobStatusQueryResponse();

    String kafkaTopicName = Version.composeKafkaTopic(store, versionNumber);

    Admin.OfflinePushStatusInfo offlineJobStatus =
        admin.getOffLinePushStatus(cluster, kafkaTopicName, incrementalPushVersion, region, targetedRegions);
    responseObject.setStatus(offlineJobStatus.getExecutionStatus().toString());
    responseObject.setStatusUpdateTimestamp(offlineJobStatus.getStatusUpdateTimestamp());
    responseObject.setStatusDetails(offlineJobStatus.getStatusDetails());
    responseObject.setExtraInfo(offlineJobStatus.getExtraInfo());
    responseObject.setExtraInfoUpdateTimestamp(offlineJobStatus.getExtraInfoUpdateTimestamp());
    responseObject.setExtraDetails(offlineJobStatus.getExtraDetails());
    responseObject.setUncompletedPartitions(offlineJobStatus.getUncompletedPartitions());

    responseObject.setCluster(cluster);
    responseObject.setName(store);
    responseObject.setVersion(versionNumber);
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
