package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;
import static com.linkedin.venice.meta.Version.PushType;


/**
 * This class will add a new version to the given store.
 */
public class CreateVersion extends AbstractRoute {
  private static final Logger logger = Logger.getLogger(CreateVersion.class);

  private final boolean checkReadMethodForKafka;

  public CreateVersion(Optional<DynamicAccessController> accessController, boolean checkReadMethodForKafka) {
    super(accessController);
    this.checkReadMethodForKafka = checkReadMethodForKafka;
  }

  /**
   * Instead of asking Venice to create a version, pushes should ask venice which topic to write into.
   * The logic below includes the ability to respond with an existing topic for the same push, allowing requests
   * to be idempotent
   *
   * @param admin
   * @return
   */
  public Route requestTopicForPushing(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow whitelist users to run this command
        if (!isWhitelistUsers(request)
            && (!hasWriteAccessToTopic(request) || (this.checkReadMethodForKafka && !hasReadAccessToTopic(request))))
        {
          response.status(HttpStatus.SC_FORBIDDEN);
          String userId = getPrincipalId(request);
          String storeName = request.queryParams(NAME);

          /**
           * When partners have ACL issues for their push, we should provide an accurate and informative messages that
           * help partners to unblock by themselves.
           */
          String errorMsg;
          boolean missingWriteAccess = !hasWriteAccessToTopic(request);
          boolean missingReadAccess = this.checkReadMethodForKafka && !hasReadAccessToTopic(request);
          if (missingWriteAccess && missingReadAccess) {
            errorMsg = "[Error] Push terminated due to ACL issues for user \"" + userId
                + "\". Please visit go/veniceacl and setup [write] ACLs for your store.";
          } else if (missingWriteAccess) {
            errorMsg = "[Error] Hadoop user \"" + userId + "\" does not have [write] permission for store: "
                + storeName + ". Please refer to go/veniceacl and setup store ACLs";
          } else {
            errorMsg = "[Error] Missing [read] method in [write] ACLs for user \"" + userId
                + "\". Please visit go/veniceacl and setup ACLs for your store";
          }
          responseObject.setError(errorMsg);
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
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

        // Retrieve partitioner config from the store
        PartitionerConfig storePartitionerConfig = store.getPartitionerConfig();

        if (null == request.queryParams(PARTITIONERS)) {
          // Request does not contain partitioner info
          responseObject.setPartitionerClass(storePartitionerConfig.getPartitionerClass());
          responseObject.setAmplificationFactor(storePartitionerConfig.getAmplificationFactor());
          responseObject.setPartitionerParams(storePartitionerConfig.getPartitionerParams());
        } else {
          // Retrieve provided partitioner class list from the request
          boolean hasMatchedPartitioner = false;
          for (String partitioner : request.queryParams(PARTITIONERS).split(",")) {
            if (partitioner.equals(storePartitionerConfig.getPartitionerClass())) {
              responseObject.setPartitionerClass(storePartitionerConfig.getPartitionerClass());
              responseObject.setAmplificationFactor(storePartitionerConfig.getAmplificationFactor());
              responseObject.setPartitionerParams(storePartitionerConfig.getPartitionerParams());
              hasMatchedPartitioner = true;
              break;
            }
          }
          if (!hasMatchedPartitioner) {
            throw new VeniceException("Expected partitioner class " + storePartitionerConfig.getPartitionerClass() + " cannot be found.");
          }
        }

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
        // Make this optional so that it is compatible with old version controller client
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

        boolean sorted = false; // an inefficient but safe default
        String sortedParam = request.queryParams(PUSH_IN_SORTED_ORDER);
        if (sortedParam != null) {
          sorted = Utils.parseBooleanFromString(sortedParam, PUSH_IN_SORTED_ORDER);
        }
        switch(pushType) {
          case BATCH:
          case INCREMENTAL:
          case STREAM_REPROCESSING:
            if (!admin.whetherEnableBatchPushFromAdmin()) {
              throw new VeniceUnsupportedOperationException(pushTypeString, "Please push data to Venice Parent Colo instead");
            }
            String dictionaryStr = request.queryParams(COMPRESSION_DICTIONARY);

            Version version =
                admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionCount, replicationFactor,
                   pushType, sendStartOfPush, sorted, dictionaryStr);

            // If Version partition count different from calculated partition count use the version count as store count
            // may have been updated later.
            if (version.getPartitionCount() != partitionCount) {
              responseObject.setPartitions(version.getPartitionCount());
            }
            String responseTopic;
            if(pushType.isStreamReprocessing()) {
              responseTopic = Version.composeStreamReprocessingTopic(storeName, version.getNumber());
            } else if (pushType.isIncremental() && version.getIncrementalPushPolicy().equals(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)) {
              responseTopic = admin.getRealTimeTopic(clusterName, storeName);
            } else {
              responseTopic = version.kafkaTopicName();
            }

            responseObject.setVersion(version.getNumber());
            responseObject.setKafkaTopic(responseTopic);
            responseObject.setCompressionStrategy(version.getCompressionStrategy());
            if (version.isNativeReplicationEnabled()) {
              String childDataCenterKafkaBootstrapServer = admin.getNativeReplicationKafkaBootstrapServer(clusterName);
              if (childDataCenterKafkaBootstrapServer != null) {
                responseObject.setKafkaBootstrapServers(childDataCenterKafkaBootstrapServer);
              }
            }
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

      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  /**
   * This function is only being used by store migration parent controllers, which write add version admin message.
   */
  public Route addVersionAndStartIngestion(Admin admin) {
    return (request, response) -> {
      VersionResponse responseObject = new VersionResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow whitelist users to run this command
        if (!isWhitelistUsers(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("ACL failed for request " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, ADD_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String pushJobId = request.queryParams(PUSH_JOB_ID);
        int versionNumber = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        int partitionCount = Utils.parseIntFromString(request.queryParams(PARTITION_COUNT), PARTITION_COUNT);
        PushType pushType;
        try {
          pushType = PushType.valueOf(request.queryParams(PUSH_TYPE));
        } catch (RuntimeException parseException) {
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, request.queryParams(PUSH_TYPE) + " is an invalid "
              + PUSH_TYPE, parseException);
        }
        String remoteKafkaBootstrapServers = null;
        if (request.queryParams().contains(REMOTE_KAFKA_BOOTSTRAP_SERVERS)) {
          remoteKafkaBootstrapServers = request.queryParams(REMOTE_KAFKA_BOOTSTRAP_SERVERS);
        }

        admin.addVersionAndStartIngestion(clusterName, storeName, pushJobId, versionNumber, partitionCount, pushType, remoteKafkaBootstrapServers);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  @Deprecated
  public Route uploadPushInfo(Admin admin){
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow whitelist users to run this command
        if (!isWhitelistUsers(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("ACL failed for request " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, OFFLINE_PUSH_INFO.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        // TODO No-op, can be removed once the corresponding H2V plugin version is deployed.
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);

    };
  }

  public Route writeEndOfPush(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow whitelist users to run this command
        if (!isWhitelistUsers(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("You don't have permission to end this push job; please grant write ACL for yourself.");
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
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
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route emptyPush(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        if (!admin.whetherEnableBatchPushFromAdmin()) {
          throw new VeniceUnsupportedOperationException("EMPTY PUSH",
              "Please push data to Venice Parent Colo instead or use Aggregate mode if you are running Samza GF Job.");
        }

        AdminSparkServer.validateParams(request, EMPTY_PUSH.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        String pushJobId = request.queryParams(PUSH_JOB_ID);
        int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        Version version =
            admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionNum, replicationFactor);
        int versionNumber = version.getNumber();

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
        responseObject.setPartitions(partitionNum);
        responseObject.setReplicas(replicationFactor);
        responseObject.setKafkaTopic(version.kafkaTopicName());

        admin.writeEndOfPush(clusterName, storeName, versionNumber, true);

        /** TODO: Poll {@link com.linkedin.venice.controller.VeniceParentHelixAdmin#getOffLineJobStatus(String, String, Map, TopicManager)} until it is terminal... */

      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route newZkSharedStoreVersion(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      response.type(HttpConstants.JSON);
      try {
        AdminSparkServer.validateParams(request, NEW_ZK_SHARED_STORE_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String zkSharedStoreName = request.queryParams(NAME);

        Version version = admin.newZkSharedStoreVersion(clusterName, zkSharedStoreName);

        responseObject.setCluster(clusterName);
        responseObject.setName(zkSharedStoreName);
        responseObject.setVersion(version.getNumber());
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route materializeMetadataStoreVersion(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        AdminSparkServer.validateParams(request, MATERIALIZE_METADATA_STORE_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int versionNumber = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);

        admin.materializeMetadataStoreVersion(clusterName, storeName, versionNumber);

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
