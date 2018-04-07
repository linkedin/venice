package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;

public class JobRoutes {
  private static final Logger logger = Logger.getLogger(JobRoutes.class);

  public static Route jobStatus(Admin admin) {
    return (request, response) -> {
      JobStatusQueryResponse responseObject = new JobStatusQueryResponse();
      try {
        AdminSparkServer.validateParams(request, JOB.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String store = request.queryParams(NAME);
        int versionNumber = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        responseObject = populateJobStatus(cluster, store, versionNumber, admin);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  protected static JobStatusQueryResponse populateJobStatus(String cluster, String store, int versionNumber, Admin admin) {
    JobStatusQueryResponse responseObject = new JobStatusQueryResponse();

    Version version = new Version(store, versionNumber);
    String kafkaTopicName = version.kafkaTopicName();

    //Available offsets
    Map<Integer, Long> offsets;
    try {
      offsets = admin.getTopicManager().getLatestOffsets(kafkaTopicName);
    } catch (Exception ex) {
      logger.error("Failed to get latest offsets from Kafka for topic: " + kafkaTopicName, ex);
      offsets = new HashMap<>();
    }
    int replicationFactor = admin.getReplicationFactor(cluster, store);
    int clusterCount = admin.getDatacenterCount(cluster);
    long aggregateOffsets = 0;
    for (Long offset : offsets.values()) {
      aggregateOffsets += offset * replicationFactor * clusterCount;
    }
    responseObject.setMessagesAvailable(aggregateOffsets);
    responseObject.setPerPartitionCapacity(offsets);

    //Current offsets
    Map<String, Long> currentProgress = admin.getOfflinePushProgress(cluster, kafkaTopicName);
    responseObject.setPerTaskProgress(currentProgress);

    //Aggregated progress
    long aggregatedProgress = 0L;
    for (Long taskOffset : currentProgress.values()) {
      aggregatedProgress += taskOffset;
    }
    responseObject.setMessagesConsumed(aggregatedProgress);

    /**
     * Job status
     * Job status query should happen after 'querying offset' since job status query could
     * delete current topic
     */
    Admin.OfflinePushStatusInfo offlineJobStatus = admin.getOffLinePushStatus(cluster, kafkaTopicName);
    responseObject.setStatus(offlineJobStatus.getExecutionStatus().toString());
    responseObject.setStatusDetails(offlineJobStatus.getStatusDetails().orElse(null));
    responseObject.setExtraInfo(offlineJobStatus.getExtraInfo());
    responseObject.setExtraDetails(offlineJobStatus.getExtraDetails());

    //TODO: available offsets finalized
    responseObject.setAvailableFinal(false);

    responseObject.setCluster(cluster);
    responseObject.setName(store);
    responseObject.setVersion(versionNumber);
    return responseObject;
  }

  public static Route killOfflinePushJob(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      try {
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
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
