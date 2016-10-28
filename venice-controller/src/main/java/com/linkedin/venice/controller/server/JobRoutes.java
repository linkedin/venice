package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TOPIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.JOB;
import static com.linkedin.venice.controllerapi.ControllerRoute.KILL_OFFLINE_PUSH_JOB;

public class JobRoutes {
  public static Route jobStatus(Admin admin) {
    return (request, response) -> {
      JobStatusQueryResponse responseObject = new JobStatusQueryResponse();
      try {
        AdminSparkServer.validateParams(request, JOB.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String store = request.queryParams(NAME);
        int versionNumber = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        responseObject = populateJobStatus(cluster, store, versionNumber, admin);
      } catch (VeniceException e) {
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

    //Job status
    String jobStatus = admin.getOffLineJobStatus(cluster, version.kafkaTopicName()).toString();
    responseObject.setStatus(jobStatus);

    //Available offsets
    Map<Integer, Long> offsets = admin.getTopicManager().getLatestOffsets(version.kafkaTopicName());
    int replicationFactor = admin.getReplicationFactor(cluster, store);
    int clusterCount = admin.getDatacenterCount(cluster);
    long aggregateOffsets = 0;
    for (Long offset : offsets.values()) {
      aggregateOffsets += offset * replicationFactor * clusterCount;
    }
    responseObject.setMessagesAvailable(aggregateOffsets);
    responseObject.setPerPartitionCapacity(offsets);

    //Current offsets
    Map<String, Long> currentProgress = admin.getOfflineJobProgress(cluster, version.kafkaTopicName());
    responseObject.setPerTaskProgress(currentProgress);

    //Aggregated progress
    long aggregatedProgress = 0L;
    for (Long taskOffset : currentProgress.values()){
      aggregatedProgress += taskOffset;
    }
    responseObject.setMessagesConsumed(aggregatedProgress);

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

        admin.killOfflineJob(cluster, topic);
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
