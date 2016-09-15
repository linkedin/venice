package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import org.apache.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.CREATE_VERSION;


/**
 * This class will add a new version to the given store.
 */
public class CreateVersion {
  private static final Logger logger = Logger.getLogger(CreateVersion.class);

  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        AdminSparkServer.validateParams(request, CREATE_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        // TODO we should verify the data size at first. If it exceeds the quota, controller should reject this request.
        // TODO And also we should use quota to calculate partition count to avoid this case that data size of first
        // push is very small but grow dramatically because quota of this store is very large.
        // Store size in Bytes
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        int replicaFactor = admin.getReplicationFactor(clusterName, storeName);
        Version version = admin.incrementVersion(clusterName, storeName, partitionNum, replicaFactor);
        // The actual partition number could be different from the one calculated here,
        // since Venice is not using dynamic partition number across different versions.
        responseObject.setPartitions(admin.getStore(clusterName, storeName).getPartitionCount());
        responseObject.setReplicas(replicaFactor);
        responseObject.setVersion(version.getNumber());
        responseObject.setKafkaTopic(version.kafkaTopicName());
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers());
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
