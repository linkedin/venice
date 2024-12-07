package com.linkedin.venice.controller.server;

import static com.linkedin.venice.ConfigKeys.EMERGENCY_SOURCE_REGION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.COMPRESSION_DICTIONARY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DEFER_VERSION_SWAP;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.IS_WRITE_COMPUTE_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONERS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITION_COUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_IN_SORTED_ORDER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_JOB_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_TYPE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REMOTE_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_METADATA_VERSION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPUSH_SOURCE_VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEND_START_OF_PUSH;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEPARATE_REAL_TIME_TOPIC_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGETED_REGIONS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.ADD_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.EMPTY_PUSH;
import static com.linkedin.venice.controllerapi.ControllerRoute.END_OF_PUSH;
import static com.linkedin.venice.controllerapi.ControllerRoute.OFFLINE_PUSH_INFO;
import static com.linkedin.venice.controllerapi.ControllerRoute.REQUEST_TOPIC;
import static com.linkedin.venice.meta.Version.PushType;
import static com.linkedin.venice.meta.Version.REPLICATION_METADATA_VERSION_ID_UNSET;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.security.cert.X509Certificate;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Route;


/**
 * This class will add a new version to the given store.
 */
public class CreateVersion extends AbstractRoute {
  private static final Logger LOGGER = LogManager.getLogger(CreateVersion.class);
  private final boolean checkReadMethodForKafka;
  private final boolean disableParentRequestTopicForStreamPushes;

  public CreateVersion(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      boolean checkReadMethodForKafka,
      boolean disableParentRequestTopicForStreamPushes) {
    super(sslEnabled, accessController);
    this.checkReadMethodForKafka = checkReadMethodForKafka;
    this.disableParentRequestTopicForStreamPushes = disableParentRequestTopicForStreamPushes;
  }

  /**
   * Instead of asking Venice to create a version, pushes should ask venice which topic to write into.
   * The logic below includes the ability to respond with an existing topic for the same push, allowing requests
   * to be idempotent.
   */
  public Route requestTopicForPushing(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow allowList users to run this command
        if (!isAllowListUser(request)
            && (!hasWriteAccessToTopic(request) || (this.checkReadMethodForKafka && !hasReadAccessToTopic(request)))) {
          response.status(HttpStatus.SC_FORBIDDEN);
          String userId = getPrincipalId(request);

          /**
           * When partners have ACL issues for their push, we should provide an accurate and informative messages that
           * help partners to unblock by themselves.
           */
          String errorMsg;
          boolean missingWriteAccess = !hasWriteAccessToTopic(request);
          boolean missingReadAccess = this.checkReadMethodForKafka && !hasReadAccessToTopic(request);
          if (missingWriteAccess && missingReadAccess) {
            errorMsg = "[Error] Missing [read] and [write] ACLs for user \"" + userId
                + "\". Please setup ACLs for your store.";
          } else if (missingWriteAccess) {
            errorMsg = "[Error] Missing [write] ACLs for user \"" + userId + "\". Please setup ACLs for your store.";
          } else {
            errorMsg = "[Error] Missing [read] ACLs for user \"" + userId + "\". Please setup ACLs for your store.";
          }
          responseObject.setError(errorMsg);
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }

        AdminSparkServer.validateParams(request, REQUEST_TOPIC.getParams(), admin);

        // Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        Store store = admin.getStore(clusterName, storeName);
        if (store == null) {
          throw new VeniceNoStoreException(storeName);
        }
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setDaVinciPushStatusStoreEnabled(store.isDaVinciPushStatusStoreEnabled());

        // Retrieve partitioner config from the store
        PartitionerConfig storePartitionerConfig = store.getPartitionerConfig();
        if (request.queryParams(PARTITIONERS) == null) {
          // Request does not contain partitioner info
          responseObject.setPartitionerClass(storePartitionerConfig.getPartitionerClass());
          responseObject.setAmplificationFactor(storePartitionerConfig.getAmplificationFactor());
          responseObject.setPartitionerParams(storePartitionerConfig.getPartitionerParams());
        } else {
          // Retrieve provided partitioner class list from the request
          boolean hasMatchedPartitioner = false;
          for (String partitioner: request.queryParams(PARTITIONERS).split(",")) {
            if (partitioner.equals(storePartitionerConfig.getPartitionerClass())) {
              responseObject.setPartitionerClass(storePartitionerConfig.getPartitionerClass());
              responseObject.setAmplificationFactor(storePartitionerConfig.getAmplificationFactor());
              responseObject.setPartitionerParams(storePartitionerConfig.getPartitionerParams());
              hasMatchedPartitioner = true;
              break;
            }
          }
          if (!hasMatchedPartitioner) {
            throw new VeniceException(
                "Expected partitioner class " + storePartitionerConfig.getPartitionerClass() + " cannot be found.");
          }
        }

        String pushTypeString = request.queryParams(PUSH_TYPE);
        PushType pushType;
        try {
          pushType = PushType.valueOf(pushTypeString);
        } catch (RuntimeException e) {
          throw new VeniceHttpException(
              HttpStatus.SC_BAD_REQUEST,
              pushTypeString + " is an invalid " + PUSH_TYPE,
              e,
              ErrorType.BAD_REQUEST);
        }
        validatePushType(pushType, store);

        boolean sendStartOfPush = false;
        // Make this optional so that it is compatible with old version controller client
        if (request.queryParams().contains(SEND_START_OF_PUSH)) {
          sendStartOfPush = Utils.parseBooleanFromString(request.queryParams(SEND_START_OF_PUSH), SEND_START_OF_PUSH);
        }

        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        int partitionCount = admin.calculateNumberOfPartitions(clusterName, storeName);
        responseObject.setReplicas(replicationFactor);
        responseObject.setPartitions(partitionCount);

        boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers(isSSL));
        responseObject.setKafkaSourceRegion(admin.getRegionName());
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
         * We can't honor source grid fabric and emergency source region config untill the store is A/A enabled in all regions. This is because
         * if push job start producing to a different prod region then non A/A enabled region will not have the capability to consume from that region.
         * This resets this config in such cases.
         */
        Lazy<Boolean> isActiveActiveReplicationEnabledInAllRegion = Lazy.of(() -> {
          if (admin.isParent() && store.isActiveActiveReplicationEnabled()) {
            return admin.isActiveActiveReplicationEnabledInAllRegion(clusterName, storeName, false);
          } else {
            return false;
          }
        });

        Lazy<Boolean> isActiveActiveReplicationEnabledInAllRegionAllVersions = Lazy.of(() -> {
          if (admin.isParent() && store.isActiveActiveReplicationEnabled()) {
            return admin.isActiveActiveReplicationEnabledInAllRegion(clusterName, storeName, true);
          } else {
            return false;
          }
        });

        if (sourceGridFabric.isPresent() && !isActiveActiveReplicationEnabledInAllRegion.get()) {
          LOGGER.info(
              "Ignoring config {} : {}, as store {} is not set up for Active/Active replication in all regions",
              SOURCE_GRID_FABRIC,
              sourceGridFabric.get(),
              storeName);
          sourceGridFabric = Optional.empty();
        }
        Optional<String> emergencySourceRegion = admin.getEmergencySourceRegion(clusterName);
        if (emergencySourceRegion.isPresent() && !isActiveActiveReplicationEnabledInAllRegion.get()) {
          LOGGER.info(
              "Ignoring config {} : {}, as store {} is not set up for Active/Active replication in all regions",
              EMERGENCY_SOURCE_REGION,
              emergencySourceRegion.get(),
              storeName);
        }
        LOGGER.info(
            "requestTopicForPushing: source grid fabric: {}, emergency source region: {}",
            sourceGridFabric.orElse(""),
            emergencySourceRegion.orElse(""));

        /**
         * Version-level rewind time override, and it is only valid for hybrid stores.
         */
        Optional<String> rewindTimeInSecondsOverrideOptional =
            Optional.ofNullable(request.queryParams(REWIND_TIME_IN_SECONDS_OVERRIDE));
        long rewindTimeInSecondsOverride = -1;
        if (rewindTimeInSecondsOverrideOptional.isPresent()) {
          rewindTimeInSecondsOverride = Long.parseLong(rewindTimeInSecondsOverrideOptional.get());
        }

        /**
         * Version level override to defer marking this new version to the serving version post push completion.
         */
        boolean deferVersionSwap = Boolean.parseBoolean(request.queryParams(DEFER_VERSION_SWAP));

        String targetedRegions = request.queryParams(TARGETED_REGIONS);

        int repushSourceVersion = Integer.parseInt(request.queryParamOrDefault(REPUSH_SOURCE_VERSION, "-1"));

        switch (pushType) {
          case BATCH:
          case INCREMENTAL:
          case STREAM_REPROCESSING:
            if (!admin.whetherEnableBatchPushFromAdmin(storeName)) {
              throw new VeniceUnsupportedOperationException(
                  pushTypeString,
                  "Please push data to Venice Parent Colo instead");
            }
            String dictionaryStr = request.queryParams(COMPRESSION_DICTIONARY);

            /**
             * Before trying to get the version, create the RT topic in parent kafka since it's needed anyway in following cases.
             * Otherwise topic existence check fails internally.
             */
            if (pushType.isIncremental() && isWriteComputeEnabled) {
              admin.getRealTimeTopic(clusterName, storeName);
            }

            final Optional<X509Certificate> certInRequest =
                isAclEnabled() ? Optional.of(getCertificate(request)) : Optional.empty();
            final Version version = admin.incrementVersionIdempotent(
                clusterName,
                storeName,
                pushJobId,
                partitionCount,
                replicationFactor,
                pushType,
                sendStartOfPush,
                sorted,
                dictionaryStr,
                sourceGridFabric,
                certInRequest,
                rewindTimeInSecondsOverride,
                emergencySourceRegion,
                deferVersionSwap,
                targetedRegions,
                repushSourceVersion);

            // If Version partition count different from calculated partition count use the version count as store count
            // may have been updated later.
            if (version.getPartitionCount() != partitionCount) {
              responseObject.setPartitions(version.getPartitionCount());
            }
            String responseTopic;
            /**
             * Override the source fabric to respect the native replication source fabric selection.
             */
            boolean overrideSourceFabric = true;
            boolean isTopicRT = false;
            if (pushType.isStreamReprocessing()) {
              responseTopic = Version.composeStreamReprocessingTopic(storeName, version.getNumber());
            } else if (pushType.isIncremental()) {
              isTopicRT = true;
              if (version.isSeparateRealTimeTopicEnabled()
                  && Boolean.parseBoolean(request.queryParamOrDefault(SEPARATE_REAL_TIME_TOPIC_ENABLED, "false"))) {
                admin.getSeparateRealTimeTopic(clusterName, storeName);
                responseTopic = Version.composeSeparateRealTimeTopic(storeName);
              } else {
                responseTopic = Version.composeRealTimeTopic(storeName);
              }
              // disable amplificationFactor logic on real-time topic
              responseObject.setAmplificationFactor(1);

              if (version.isNativeReplicationEnabled()) {
                /**
                 * For incremental push with RT policy store the push job produces to parent corp kafka cluster. We should not override the
                 * source fabric in such cases with NR source fabric.
                 */
                overrideSourceFabric = false;
              }
            } else {
              responseTopic = version.kafkaTopicName();
            }

            responseObject.setVersion(version.getNumber());
            responseObject.setKafkaTopic(responseTopic);
            if (isTopicRT) {
              // RT topic only supports NO_OP compression
              responseObject.setCompressionStrategy(CompressionStrategy.NO_OP);
            } else {
              responseObject.setCompressionStrategy(version.getCompressionStrategy());
            }
            if (version.isNativeReplicationEnabled() && overrideSourceFabric) {
              String childDataCenterKafkaBootstrapServer = version.getPushStreamSourceAddress();
              if (childDataCenterKafkaBootstrapServer != null) {
                responseObject.setKafkaBootstrapServers(childDataCenterKafkaBootstrapServer);
              }
              responseObject.setKafkaSourceRegion(version.getNativeReplicationSourceFabric());
            }

            if (pushType.isIncremental() && admin.isParent()) {
              overrideSourceRegionAddressForIncrementalPushJob(
                  admin,
                  responseObject,
                  clusterName,
                  emergencySourceRegion.orElse(null),
                  sourceGridFabric.orElse(null),
                  isActiveActiveReplicationEnabledInAllRegion.get(),
                  version.isNativeReplicationEnabled());
              LOGGER.info(
                  "Incremental push job final source region address is: {}",
                  responseObject.getKafkaBootstrapServers());
            }
            break;
          case STREAM:

            if (admin.isParent()) {

              // Conditionally check if the controller allows for fetching this information
              if (disableParentRequestTopicForStreamPushes) {
                throw new VeniceException(
                    String.format(
                        "Parent request topic is disabled!!  Cannot push data to topic in parent colo for store %s.  Aborting!!",
                        storeName));
              }

              // Conditionally check if this store has aggregate mode enabled. If not, throw an exception (as aggregate
              // mode is required to produce to parent colo)
              // We check the store config instead of the version config because we want this policy to go into affect
              // without needing to perform empty pushes everywhere
              if (!store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)) {
                if (!isActiveActiveReplicationEnabledInAllRegionAllVersions.get()) {
                  throw new VeniceException("Store is not in aggregate mode!  Cannot push data to parent topic!!");
                } else {
                  LOGGER.info(
                      "Store: {} samza job running in Aggregate mode, Store config is in Non-Aggregate mode, "
                          + "AA is enabled in all regions, letting the job continue",
                      storeName);
                }
              }
            } else {
              if (store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)) {
                if (!store.isActiveActiveReplicationEnabled()) {
                  throw new VeniceException("Store is in aggregate mode!  Cannot push data to child topic!!");
                } else {
                  LOGGER.info(
                      "Store: {} samza job running in Non-Aggregate mode, Store config is in Aggregate mode, "
                          + "AA is enabled in the local region, letting the job continue",
                      storeName);
                }
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
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }

      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * Override the source fabric [response.setKafkaBootstrapServers()] for VPJ incremental push job for A/A replication.
   * Following is the order of priority.
   *  P1. Parent controller emergency source fabric config.
   *  P2. VPJ plugin source grid fabric config.
   *  P3. parent corp kafka cluster.
   *
   * At this point parent corp cluster is already set in the response.setKafkaBootstrapServers().
   * So we only need to override for P1 and P2.
   */
  // visible for testing
  static void overrideSourceRegionAddressForIncrementalPushJob(
      Admin admin,
      VersionCreationResponse response,
      String clusterName,
      String emergencySourceRegion,
      String pushJobSourceGridFabric,
      boolean isAAEnabledInAllRegions,
      boolean isNativeReplicationEnabled) {
    if (!isAAEnabledInAllRegions && isNativeReplicationEnabled) {
      // P2: When AA is not enabled in all the regions we use aggregate RT address, if it is available,
      // for inc-pushes if native-replication is enabled.
      admin.getAggregateRealTimeTopicSource(clusterName).ifPresent(response::setKafkaBootstrapServers);
      return;
    } else if (!isAAEnabledInAllRegions) {
      // When AA is not enabled in all regions and native replication is also disabled, don't do anything.
      return;
    }

    String overRideSourceRegion = emergencySourceRegion;
    // When emergencySourceRegion is null then use sourceGridFabric
    if (overRideSourceRegion == null && pushJobSourceGridFabric != null) {
      overRideSourceRegion = pushJobSourceGridFabric;
    }
    // When emergencySourceRegion is not set and VPJ did not send SOURCE_GRID_FABRIC
    // then we do not override the bootstrap server address.
    if (overRideSourceRegion == null) {
      return;
    }
    String bootstrapServerAddress = admin.getNativeReplicationKafkaBootstrapServerAddress(overRideSourceRegion);
    if (bootstrapServerAddress == null) {
      throw new VeniceException("Failed to get the broker server URL for the source region: " + overRideSourceRegion);
    }
    LOGGER.info(
        "Incremental push job source region is being overridden with: {} address: {}",
        overRideSourceRegion,
        bootstrapServerAddress);
    response.setKafkaBootstrapServers(bootstrapServerAddress);
  }

  void validatePushType(PushType pushType, Store store) {
    if (pushType.equals(PushType.STREAM) && !store.isHybrid()) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "requesting topic for streaming writes to store " + store.getName()
              + " which is not configured to be a hybrid store",
          ErrorType.BAD_REQUEST);
    }
    /**
     * Allow STREAM push type if one of the following conditions is true
     * 1. AA is enabled for the store
     * 2. AA is not enabled for the store but the store is configured with NON_AGGREGATE data replication policy
     * 3. AA is not enabled for the store but the store is configured with AGGREGATE data replication policy
     */
    if (pushType.equals(PushType.STREAM) && !(store.isActiveActiveReplicationEnabled()
        || store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.NON_AGGREGATE)
        || store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE))) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "requesting topic for streaming writes to store " + store.getName()
              + " which is configured to have a hybrid data replication policy "
              + store.getHybridStoreConfig().getDataReplicationPolicy(),
          ErrorType.BAD_REQUEST);
    }
    if (pushType.isIncremental() && !store.isHybrid()) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "requesting topic for incremental push to store: " + store.getName()
              + " which does not have hybrid mode enabled.",
          ErrorType.BAD_REQUEST);
    }
    if (pushType.isIncremental() && !store.isIncrementalPushEnabled()) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "requesting topic for incremental push to store: " + store.getName()
              + " which does not have incremental push enabled.",
          ErrorType.BAD_REQUEST);
    }
  }

  /**
   * This function is only being used by store migration parent controllers, which write add version admin message.
   * @see Admin#addVersionAndStartIngestion(String, String, String, int, int, PushType, String, long, int, boolean, int)
   */
  public Route addVersionAndStartIngestion(Admin admin) {
    return (request, response) -> {
      VersionResponse responseObject = new VersionResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow allowlist users to run this command
        if (!isAllowListUser(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("ACL failed for request " + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
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
          throw new VeniceHttpException(
              HttpStatus.SC_BAD_REQUEST,
              request.queryParams(PUSH_TYPE) + " is an invalid " + PUSH_TYPE,
              parseException,
              ErrorType.BAD_REQUEST);
        }
        String remoteKafkaBootstrapServers = null;
        if (request.queryParams().contains(REMOTE_KAFKA_BOOTSTRAP_SERVERS)) {
          remoteKafkaBootstrapServers = request.queryParams(REMOTE_KAFKA_BOOTSTRAP_SERVERS);
        }

        /**
         * Version-level rewind time override, and it is only valid for hybrid stores.
         */
        Optional<String> rewindTimeInSecondsOverrideOptional =
            Optional.ofNullable(request.queryParams(REWIND_TIME_IN_SECONDS_OVERRIDE));
        long rewindTimeInSecondsOverride = -1;
        if (rewindTimeInSecondsOverrideOptional.isPresent()) {
          rewindTimeInSecondsOverride = Long.parseLong(rewindTimeInSecondsOverrideOptional.get());
        }

        Optional<String> replicationMetadataVersionIdOptional =
            Optional.ofNullable(request.queryParams(REPLICATION_METADATA_VERSION_ID));
        int replicationMetadataVersionId = REPLICATION_METADATA_VERSION_ID_UNSET;
        if (replicationMetadataVersionIdOptional.isPresent()) {
          replicationMetadataVersionId = Integer.parseInt(replicationMetadataVersionIdOptional.get());
        }

        admin.addVersionAndStartIngestion(
            clusterName,
            storeName,
            pushJobId,
            versionNumber,
            partitionCount,
            pushType,
            remoteKafkaBootstrapServers,
            rewindTimeInSecondsOverride,
            replicationMetadataVersionId,
            false,
            -1);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  @Deprecated
  public Route uploadPushInfo(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow allowlist users to run this command
        if (!isAllowListUser(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("ACL failed for request " + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, OFFLINE_PUSH_INFO.getParams(), admin);

        // Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        // TODO No-op, can be removed once the corresponding VPJ plugin version is deployed.
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);

    };
  }

  /**
   * @see Admin#writeEndOfPush(String, String, int, boolean)
   */
  public Route writeEndOfPush(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Also allow allowlist users to run this command
        if (!isAllowListUser(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject
              .setError("You don't have permission to end this push job; please grant write ACL for yourself.");
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, END_OF_PUSH.getParams(), admin);

        // Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String versionString = request.queryParams(VERSION);
        int versionNumber = Integer.parseInt(versionString);

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        admin.writeEndOfPush(clusterName, storeName, versionNumber, false);

      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  public Route emptyPush(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      response.type(HttpConstants.JSON);
      String clusterName = null;
      Version version = null;
      try {
        // Only allow allowlist users to run this command
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, EMPTY_PUSH.getParams(), admin);

        String storeName = request.queryParams(NAME);
        if (!admin.whetherEnableBatchPushFromAdmin(storeName)) {
          throw new VeniceUnsupportedOperationException(
              "EMPTY PUSH",
              "Please push data to Venice Parent Colo instead or use Aggregate mode if you are running Samza GF Job.");
        }

        clusterName = request.queryParams(CLUSTER);
        String pushJobId = request.queryParams(PUSH_JOB_ID);
        int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName);
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        version = admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionNum, replicationFactor);
        int versionNumber = version.getNumber();

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
        responseObject.setPartitions(partitionNum);
        responseObject.setReplicas(replicationFactor);
        responseObject.setKafkaTopic(version.kafkaTopicName());
        responseObject.setKafkaBootstrapServers(version.getPushStreamSourceAddress());

        admin.writeEndOfPush(clusterName, storeName, versionNumber, true);

        /** TODO: Poll {@link com.linkedin.venice.controller.VeniceParentHelixAdmin#getOffLineJobStatus(String, String, Map, TopicManager)} until it is terminal... */

      } catch (Throwable e) {
        // Clean up on failed push.
        if (version != null && clusterName != null) {
          LOGGER.warn("Cleaning up failed Empty push of {}", version.kafkaTopicName());
          admin.killOfflinePush(clusterName, version.kafkaTopicName(), true);
        }
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
