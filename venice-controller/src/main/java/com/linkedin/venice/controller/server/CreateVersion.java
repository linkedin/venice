package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


/**
 * This class will add a new version to the given store.
 */
public class CreateVersion {
  private static final Logger logger = Logger.getLogger(CreateVersion.class);

  /**
   * Instead of asking Venice to create a version, pushes should ask venice which topic to write into.
   * The logic below includes the ability to respond with an existing topic for the same push, allowing requests
   * to be idempotent
   *
   * @param admin
   * @return
   */
  public static Route requestTopicForPushing(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        AdminSparkServer.validateParams(request, REQUEST_TOPIC.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        Store store = admin.getStore(clusterName, storeName);
        if (null == store) {
          throw new VeniceNoStoreException(storeName);
        }
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        String pushTypeString = request.queryParams(PUSH_TYPE);
        PushType pushType;
        try {
          pushType = PushType.valueOf(pushTypeString);
        } catch (RuntimeException e){
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, pushTypeString + " is an invalid " + PUSH_TYPE, e);
        }

        if (pushType.equals(PushType.STREAM) && !store.isHybrid()){
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "requesting topic for streaming writes to store "
              + storeName + " which is not configured to be a hybrid store");
        }

        boolean sendStartOfPush = false;
        // Make this optional so that it is compatible with old version controller clien
        if (request.queryParams().contains(SEND_START_OF_PUSH)) {
          sendStartOfPush = Utils.parseBooleanFromString(request.queryParams(SEND_START_OF_PUSH), SEND_START_OF_PUSH);
        }

        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        int partitionCount = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        responseObject.setReplicas(replicationFactor);
        responseObject.setPartitions(partitionCount);

        boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers(isSSL));
        responseObject.setEnableSSL(isSSL);

        String pushJobId = request.queryParams(PUSH_JOB_ID);
        switch(pushType) {
          case BATCH:
          case INCREMENTAL:
            Version version =
                admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionCount, replicationFactor,
                    true, (pushType == PushType.INCREMENTAL), sendStartOfPush);

            responseObject.setVersion(version.getNumber());
            responseObject.setKafkaTopic(version.kafkaTopicName());
            responseObject.setCompressionStrategy(version.getCompressionStrategy());
            break;
          case STREAM:
            String realTimeTopic = admin.getRealTimeTopic(clusterName, storeName);
            responseObject.setKafkaTopic(realTimeTopic);
            break;
          default:
            throw new VeniceException(pushTypeString + " is an unrecognized " + PUSH_TYPE);
        }
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }

      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public static Route uploadPushInfo(Admin admin){
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      try {
        AdminSparkServer.validateParams(request, OFFLINE_PUSH_INFO.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        String versionString = request.queryParams(VERSION);
        int versionNumber = Integer.parseInt(versionString);
        Map<String, String> properties = new HashMap<>();
        for (String key : request.queryParams()) {
          properties.put(key, request.queryParams(key));
        }
        admin.updatePushProperties(clusterName, storeName, versionNumber, properties);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);

    };
  }

  public static Route writeEndOfPush(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      try {
        AdminSparkServer.validateParams(request, END_OF_PUSH.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String versionString = request.queryParams(VERSION);
        int versionNumber = Integer.parseInt(versionString);

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        admin.writeEndOfPush(clusterName, storeName, versionNumber, false);

      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public static Route emptyPush(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        AdminSparkServer.validateParams(request, EMPTY_PUSH.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        String pushJobId = request.queryParams(PUSH_JOB_ID);
        int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        Version version =
            admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionNum, replicationFactor, true);
        int versionNumber = version.getNumber();

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
        responseObject.setPartitions(partitionNum);
        responseObject.setReplicas(replicationFactor);

        admin.writeEndOfPush(clusterName, storeName, versionNumber, true);

        /** TODO: Poll {@link com.linkedin.venice.controller.VeniceParentHelixAdmin#getOffLineJobStatus(String, String, Map, TopicManager)} until it is terminal... */

      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
