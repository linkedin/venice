package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.routes.PushJobStatusUploadResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.status.protocol.PushJobStatusRecordValue;
import com.linkedin.venice.status.protocol.enums.PushJobStatus;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.Optional;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.http.HttpStatus;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;

public class JobRoutes extends AbstractRoute {
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

  protected JobStatusQueryResponse populateJobStatus(String cluster, String store,
      int versionNumber, Admin admin, Optional<String> incrementalPushVersion) {
    JobStatusQueryResponse responseObject = new JobStatusQueryResponse();

    Version version = new Version(store, versionNumber);
    String kafkaTopicName = version.kafkaTopicName();

    /**
     * Offset progress is not being used by KafkaPushJob, and sometimes, it will timeout because of Kafka operation timeout
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
    Admin.OfflinePushStatusInfo offlineJobStatus = admin.getOffLinePushStatus(cluster, kafkaTopicName, incrementalPushVersion);
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
          responseObject.setError("You don't have permission to kill this push job; please grant write ACL for yourself.");
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, KILL_OFFLINE_PUSH_JOB.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String topic = request.queryParams(TOPIC);
        responseObject.setCluster(cluster);
        responseObject.setName(Version.parseStoreFromKafkaTopicName(topic));


        admin.killOfflinePush(cluster, topic);
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
      try {
        // No ACL check for uploading push status
        AdminSparkServer.validateParams(request, UPLOAD_PUSH_JOB_STATUS.getParams(), admin);
        responseObject.setName(request.queryParams(NAME));
        responseObject.setVersion(Utils.parseIntFromString(request.queryParams(VERSION), VERSION));
        PushJobStatusRecordKey key = new PushJobStatusRecordKey();
        PushJobStatusRecordValue value = new PushJobStatusRecordValue();
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        key.storeName = storeName;
        key.versionNumber = version;
        value.storeName = storeName;
        value.clusterName = request.queryParams(CLUSTER);
        value.versionNumber = version;
        value.status = PushJobStatus.valueOf(request.queryParams(PUSH_JOB_STATUS));
        value.pushDuration = Utils.parseLongFromString(request.queryParams(PUSH_JOB_DURATION), PUSH_JOB_DURATION);
        value.pushId = request.queryParams(PUSH_JOB_ID);
        value.message = request.queryParams(MESSAGE);
        admin.sendPushJobStatusMessage(key, value);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
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

        String pushJobDetailsString = request.queryParams(PUSH_JOB_DETAILS);
        DatumReader<PushJobDetails> reader = new SpecificDatumReader<>(PushJobDetails.SCHEMA$);
        PushJobDetails pushJobDetails =
            reader.read(null, DecoderFactory.get().jsonDecoder(PushJobDetails.SCHEMA$, pushJobDetailsString));
        admin.sendPushJobDetails(key, pushJobDetails);
      } catch (Throwable e) {
        controllerResponse.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(controllerResponse);
    });
  }
}
