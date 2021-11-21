package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.IncrementalPushVersionsResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.routes.PushJobStatusUploadResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.Utils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;

import spark.Route;

import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Optional;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class JobRoutes extends AbstractRoute {
  private static final Logger logger = Logger.getLogger(JobRoutes.class);
  private InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsSerializer =
      AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();

  public JobRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

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
        responseObject = populateJobStatus(cluster, store, versionNumber, admin,
            incrementalPushVersion == null ? Optional.empty() : Optional.of(incrementalPushVersion));
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  protected JobStatusQueryResponse populateJobStatus(String cluster, String store, int versionNumber, Admin admin,
      Optional<String> incrementalPushVersion) {
    JobStatusQueryResponse responseObject = new JobStatusQueryResponse();

    Version version = new VersionImpl(store, versionNumber);
    String kafkaTopicName = version.kafkaTopicName();

    /**
     * Offset progress is not being used by VenicePushJob, and sometimes, it will timeout because of Kafka operation timeout
     * while retrieving the latest offset.
     * So we decided to disable this feature, TODO: might remove it completely in the future.
     */
    responseObject.setMessagesAvailable(-1);
    responseObject.setPerPartitionCapacity(Collections.emptyMap());
    responseObject.setMessagesConsumed(-1);
    responseObject.setPerTaskProgress(Collections.emptyMap());

    /**
     * Job status
     * Job status query should happen after 'querying offset' since job status query could
     * delete current topic
     */
    Admin.OfflinePushStatusInfo offlineJobStatus =
        admin.getOffLinePushStatus(cluster, kafkaTopicName, incrementalPushVersion);
    responseObject.setStatus(offlineJobStatus.getExecutionStatus().toString());
    responseObject.setStatusDetails(offlineJobStatus.getStatusDetails().orElse(null));
    responseObject.setExtraInfo(offlineJobStatus.getExtraInfo());
    responseObject.setExtraDetails(offlineJobStatus.getExtraDetails());

    responseObject.setCluster(cluster);
    responseObject.setName(store);
    responseObject.setVersion(versionNumber);
    return responseObject;
  }

  public Route killOfflinePushJob(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow whitelist users to run this command
        if (!isWhitelistUsers(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError(
              "You don't have permission to kill this push job; please grant write ACL for yourself.");
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, KILL_OFFLINE_PUSH_JOB.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String topic = request.queryParams(TOPIC);
        responseObject.setCluster(cluster);
        responseObject.setName(Version.parseStoreFromKafkaTopicName(topic));

        admin.killOfflinePush(cluster, topic, false);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route uploadPushJobStatus(Admin admin) {
    return (request, response) -> {
      PushJobStatusUploadResponse responseObject = new PushJobStatusUploadResponse();
      response.type(HttpConstants.JSON);
      // TODO: remove once all h2v plugin deployments have updated and no longer calling into this for reporting.
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

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
        PushJobDetails pushJobDetails;
        // TODO remove passing PushJobDetails as JSON string once all H2V plugins are updated.
        if (request.queryParams().contains(PUSH_JOB_DETAILS)) {
          String pushJobDetailsString = request.queryParams(PUSH_JOB_DETAILS);
          DatumReader<PushJobDetails> reader = new SpecificDatumReader<>(PushJobDetails.SCHEMA$, PushJobDetails.SCHEMA$);
          pushJobDetails = reader.read(null, AvroCompatibilityHelper.newCompatibleJsonDecoder(PushJobDetails.SCHEMA$, pushJobDetailsString));
        } else {
          pushJobDetails = pushJobDetailsSerializer.deserialize(null, request.bodyAsBytes());
        }
        admin.sendPushJobDetails(key, pushJobDetails);

        if (pushJobDetails.sendLivenessHeartbeatFailureDetails != null) {
          logger.warn(String.format("Sending push job liveness heartbeats for store %s with version %d failed due to "
              + "%s. Push job ID is: %s", storeName, versionNumber, pushJobDetails.failureDetails.toString(), pushJobDetails.pushId.toString()));
        }

      } catch (Throwable e) {
        controllerResponse.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(controllerResponse);
    });
  }

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
        incrementalPushVersionsResponse.setIncrementalPushVersions(
            admin.getOngoingIncrementalPushVersions(clusterName, versionTopic));
      } catch (Throwable e) {
        incrementalPushVersionsResponse.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(incrementalPushVersionsResponse);
    });
  }
}
