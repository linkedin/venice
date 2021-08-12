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
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;
import static com.linkedin.venice.meta.Version.*;


/**
 * This class will add a new version to the given store.
 */
public class CreateVersion extends AbstractRoute {
  private static final Logger LOGGER = Logger.getLogger(CreateVersion.class);
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
        responseObject.setDaVinciPushStatusStoreEnabled(store.isDaVinciPushStatusStoreEnabled());

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

        if (!store.isLeaderFollowerModelEnabled()
            && store.getPartitionerConfig() != null && store.getPartitionerConfig().getAmplificationFactor() != 1) {
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "amplificationFactor can only be specified "
              + "when leaderFollower enabled");
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

        boolean isWriteComputeEnabled = false;
        String wcEnabledParam = request.queryParams(IS_WRITE_COMPUTE_ENABLED);
        if (wcEnabledParam != null) {
          isWriteComputeEnabled = Utils.parseBooleanFromString(wcEnabledParam, IS_WRITE_COMPUTE_ENABLED);
        }

        Optional<String> sourceGridFabric = Optional.ofNullable(request.queryParams(SOURCE_GRID_FABRIC));

        /**
         * Version-level rewind time override, and it is only valid for hybrid stores.
         */
        Optional<String> rewindTimeInSecondsOverrideOptional = Optional.ofNullable(request.queryParams(REWIND_TIME_IN_SECONDS_OVERRIDE));
        long rewindTimeInSecondsOverride = -1;
        if (rewindTimeInSecondsOverrideOptional.isPresent()) {
          rewindTimeInSecondsOverride = Long.parseLong(rewindTimeInSecondsOverrideOptional.get());
        }

        switch(pushType) {
          case BATCH:
          case INCREMENTAL:
          case STREAM_REPROCESSING:
            if (!admin.whetherEnableBatchPushFromAdmin()) {
              throw new VeniceUnsupportedOperationException(pushTypeString, "Please push data to Venice Parent Colo instead");
            }
            String dictionaryStr = request.queryParams(COMPRESSION_DICTIONARY);

            /**
             * Before trying to get the version, create the RT topic in parent kafka since it's needed anyway in following cases.
             * Otherwise topic existence check fails internally.
             */
            if (pushType.isIncremental() && (isWriteComputeEnabled || store.getIncrementalPushPolicy()
                .equals(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME))) {
              admin.getRealTimeTopic(clusterName, storeName);
              int targetVersion;
              if (admin.isParent()) {
                Map<String, Integer> regionToCurrentVersions = admin.getCurrentVersionsForMultiColos(clusterName, storeName);
                if (regionToCurrentVersions == null || regionToCurrentVersions.isEmpty()) {
                  throw new VeniceException("Failed to get current versions from different regions in parent controller "
                      + "for store " + storeName + " during incremental push");
                }
                targetVersion = regionToCurrentVersions.entrySet().iterator().next().getValue();
                for (Map.Entry<String, Integer> regionToCurrentVersion : regionToCurrentVersions.entrySet()) {
                  if (regionToCurrentVersion.getValue() != targetVersion) {
                    throw new VeniceException("Current version for store " + storeName + " is " + regionToCurrentVersion.getValue()
                        + " in region " + regionToCurrentVersion.getKey() + ", which is different from other regions. "
                        + "Failing the incremental push until there is a consistent current version across all regions");
                  }
                }
              } else {
                targetVersion = store.getCurrentVersion();
              }
              /**
               * Set the store's current version into the response object.
               */
              responseObject.setCurrentVersion(targetVersion);
            }

            final Optional<X509Certificate> certInRequest = isAclEnabled() ? Optional.of(getCertificate(request)) : Optional.empty();
            final Version version =
                admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionCount, replicationFactor,
                   pushType, sendStartOfPush, sorted, dictionaryStr, sourceGridFabric, certInRequest, rewindTimeInSecondsOverride);

            // If Version partition count different from calculated partition count use the version count as store count
            // may have been updated later.
            if (version.getPartitionCount() != partitionCount) {
              responseObject.setPartitions(version.getPartitionCount());
            }
            String responseTopic;
            boolean overrideSourceFabric = true;
            if (pushType.isStreamReprocessing()) {
              responseTopic = Version.composeStreamReprocessingTopic(storeName, version.getNumber());
            } else if (pushType.isIncremental() && version.getIncrementalPushPolicy().equals(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)) {
              responseTopic = Version.composeRealTimeTopic(storeName);
              // disable amplificationFactor logic on real-time topic
              responseObject.setAmplificationFactor(1);

              if (version.isNativeReplicationEnabled()) {
                /**
                 * For incremental push with RT policy store the push job produces to parent corp kafka cluster. We should not override the
                 * source fabric in such cases with NR source fabric.
                 */
                overrideSourceFabric = false;
              }
            } else if (pushType.isIncremental() && store.isHybrid()) {
              /** We want to check if the current version has both incremental push and buffer replay enabled but a full push has
               *  not been made. There are three possible cases of config updates that could lead to this:
               *  1. Store was already hybrid enabled and incremental push was enabled later - disallow incremental push
               *  2. Store was not incremental and not hybrid enabled, but was enabled for both at the same time - disallow incremental push
               *  3. Store was incremental enabled and hybrid was enabled later - allow incremental push
               *
               *  This 'if' block only checks if incremental and hybrid configs are enabled at the same time. We can't
               *  differentiate between the three cases since these configs are not version level configs.
               */
              throw new VeniceUnsupportedOperationException(pushTypeString,
                  "Incremental push cannot be made with hybrid stores unless the version push policy is "
                      + IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME
                      + ". If this is a recent change, please make a full push for configs to take effect.");
            } else {
              responseTopic = version.kafkaTopicName();
            }

            responseObject.setVersion(version.getNumber());
            responseObject.setKafkaTopic(responseTopic);
            responseObject.setCompressionStrategy(version.getCompressionStrategy());
            if (version.isNativeReplicationEnabled() && overrideSourceFabric) {
              String childDataCenterKafkaBootstrapServer = version.getPushStreamSourceAddress();
              if (childDataCenterKafkaBootstrapServer != null) {
                responseObject.setKafkaBootstrapServers(childDataCenterKafkaBootstrapServer);
              }
            }
            /*
              The conditions for topic switching to a real time topic are only
              when write compute is enabled and the push type is incremental. If
              the incremental push policy is INCREMENTAL_PUSH_SAME_AS_REAL_TIME, then
              no topic switch is needed because the leader storage node is already
              consuming from a real time topic.
             */
            if (isWriteComputeEnabled && pushType.isIncremental() &&
               !version.getIncrementalPushPolicy().equals(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)) {

              String realTimeTopic = Version.composeRealTimeTopic(storeName);

              VeniceWriter veniceWriter = admin.getVeniceWriterFactory()
                  .createVeniceWriter(responseObject.getKafkaTopic(), responseObject.getKafkaBootstrapServers());
              veniceWriter.broadcastTopicSwitch(Arrays.asList(responseObject.getKafkaBootstrapServers()),
                  realTimeTopic, -1L, new HashMap<>());

              veniceWriter.close();
              responseObject.setKafkaTopic(realTimeTopic);
            }
            break;
          case STREAM:

            if (admin.isParent()) {
              // Conditionally check if this store has aggregate mode enabled.  If not, throw an exception (as aggregate mode is required to produce to parent colo)
              // We check the store config instead of the version config because we want this policy to go into affect without needing to perform empty pushes everywhere
              if(!store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)) {
                throw new VeniceException("Store is not in aggregate mode!  Cannot push data to parent topic!!");
              }
            } else {
              if(store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)) {
                throw new VeniceException("Store is in aggregate mode!  Cannot push data to child topic!!");
              }
            }

            String realTimeTopic = admin.getRealTimeTopic(clusterName, storeName);
            responseObject.setKafkaTopic(realTimeTopic);
            // disable amplificationFactor logic on real-time topic
            responseObject.setAmplificationFactor(1);
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

        /**
         * Version-level rewind time override, and it is only valid for hybrid stores.
         */
        Optional<String> rewindTimeInSecondsOverrideOptional = Optional.ofNullable(request.queryParams(REWIND_TIME_IN_SECONDS_OVERRIDE));
        long rewindTimeInSecondsOverride = -1;
        if (rewindTimeInSecondsOverrideOptional.isPresent()) {
          rewindTimeInSecondsOverride = Long.parseLong(rewindTimeInSecondsOverrideOptional.get());
        }

        admin.addVersionAndStartIngestion(clusterName, storeName, pushJobId, versionNumber, partitionCount, pushType,
            remoteKafkaBootstrapServers, rewindTimeInSecondsOverride, TIMESTAMP_METADATA_VERSION_ID_UNSET);
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
        Version version = admin.incrementVersionIdempotent(clusterName, storeName, pushJobId,
            partitionNum, replicationFactor);
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
