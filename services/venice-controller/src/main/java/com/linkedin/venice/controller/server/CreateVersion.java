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
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPUSH_TTL_SECONDS;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.RequestTopicForPushRequest;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreAclException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.participant.protocol.enums.PushJobKillTrigger;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Response;
import spark.Route;


/**
 * This class will add a new version to the given store.
 */
public class CreateVersion extends AbstractRoute {
  private static final Logger LOGGER = LogManager.getLogger(CreateVersion.class);
  private final boolean checkReadMethodForKafka;

  public CreateVersion(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      boolean checkReadMethodForKafka) {
    super(sslEnabled, accessController);
    this.checkReadMethodForKafka = checkReadMethodForKafka;
  }

  protected static void extractOptionalParamsFromRequestTopicRequest(
      Request httpRequest,
      RequestTopicForPushRequest request,
      boolean isAclEnabled) {
    request.setPartitioners(httpRequest.queryParamOrDefault(PARTITIONERS, null));

    request.setSendStartOfPush(
        Utils.parseBooleanOrThrow(httpRequest.queryParamOrDefault(SEND_START_OF_PUSH, "false"), SEND_START_OF_PUSH));

    request.setSorted(
        Utils
            .parseBooleanOrThrow(httpRequest.queryParamOrDefault(PUSH_IN_SORTED_ORDER, "false"), PUSH_IN_SORTED_ORDER));

    request.setWriteComputeEnabled(
        Utils.parseBooleanOrThrow(
            httpRequest.queryParamOrDefault(IS_WRITE_COMPUTE_ENABLED, "false"),
            IS_WRITE_COMPUTE_ENABLED));

    request.setSeparateRealTimeTopicEnabled(
        Utils.parseBooleanOrThrow(
            httpRequest.queryParamOrDefault(SEPARATE_REAL_TIME_TOPIC_ENABLED, "false"),
            SEPARATE_REAL_TIME_TOPIC_ENABLED));

    /*
     * Version-level rewind time override, and it is only valid for hybrid stores.
     */
    request.setRewindTimeInSecondsOverride(
        Long.parseLong(httpRequest.queryParamOrDefault(REWIND_TIME_IN_SECONDS_OVERRIDE, "-1")));

    /*
     * Version level override to defer marking this new version to the serving version post push completion.
     */
    request.setDeferVersionSwap(
        Utils.parseBooleanOrThrow(httpRequest.queryParamOrDefault(DEFER_VERSION_SWAP, "false"), DEFER_VERSION_SWAP));

    request.setTargetedRegions(httpRequest.queryParamOrDefault(TARGETED_REGIONS, null));

    request.setRepushSourceVersion(Integer.parseInt(httpRequest.queryParamOrDefault(REPUSH_SOURCE_VERSION, "-1")));

    request.setSourceGridFabric(httpRequest.queryParamOrDefault(SOURCE_GRID_FABRIC, null));

    request.setCompressionDictionary(httpRequest.queryParamOrDefault(COMPRESSION_DICTIONARY, null));

    // Retrieve certificate from request if ACL is enabled
    request.setCertificateInRequest(isAclEnabled ? getCertificate(httpRequest) : null);

    request.setRepushTtlSeconds(Integer.parseInt(httpRequest.queryParamOrDefault(REPUSH_TTL_SECONDS, "-1")));
  }

  /**
   * Verifies that the partitioner class specified in the request is valid
   * based on the store's partitioner configuration.
   *
   * <p>
   * If no partitioners are provided (null or empty), the validation is skipped.
   * (The store's partitioner configuration is used when no partitioners are provided).
   * </p>
   *
   * @param partitionersFromRequest An optional set of partitioners provided in the request.
   * @param storePartitionerConfig The store's partitioner configuration to use for validation.
   * @throws VeniceException if the store's partitioner is not in the provided partitioners.
   */
  private static void validatePartitionerAgainstStoreConfig(
      Set<String> partitionersFromRequest,
      PartitionerConfig storePartitionerConfig) {
    // Skip validation if the user didn't provide any partitioner. Partitioner from store config will be used.
    if (partitionersFromRequest == null || partitionersFromRequest.isEmpty()) {
      return;
    }

    // Validate if the store's partitioner matches one of the provided partitioners
    if (!partitionersFromRequest.contains(storePartitionerConfig.getPartitionerClass())) {
      throw new VeniceException(
          "Expected partitioner class " + storePartitionerConfig.getPartitionerClass() + " cannot be found.");
    }
  }

  protected void verifyAndConfigurePartitionerSettings(
      PartitionerConfig storePartitionerConfig,
      Set<String> partitionersFromRequest,
      VersionCreationResponse response) {
    validatePartitionerAgainstStoreConfig(partitionersFromRequest, storePartitionerConfig);
    partitionersFromRequest = partitionersFromRequest != null ? partitionersFromRequest : Collections.emptySet();
    // Get the first partitioner that matches the store partitioner
    for (String partitioner: partitionersFromRequest) {
      if (storePartitionerConfig.getPartitionerClass().equals(partitioner)) {
        response.setPartitionerClass(partitioner);
        response.setPartitionerParams(storePartitionerConfig.getPartitionerParams());
        response.setAmplificationFactor(storePartitionerConfig.getAmplificationFactor());
        return;
      }
    }
    response.setPartitionerClass(storePartitionerConfig.getPartitionerClass());
    response.setPartitionerParams(storePartitionerConfig.getPartitionerParams());
    response.setAmplificationFactor(storePartitionerConfig.getAmplificationFactor());
  }

  protected Lazy<Boolean> getActiveActiveReplicationCheck(
      Admin admin,
      Store store,
      String clusterName,
      String storeName,
      boolean checkCurrentVersion) {
    return Lazy.of(
        () -> admin.isParent() && store.isActiveActiveReplicationEnabled()
            && admin.isActiveActiveReplicationEnabledInAllRegion(clusterName, storeName, checkCurrentVersion));
  }

  protected String applyConfigBasedOnReplication(
      String configType,
      String configValue,
      String storeName,
      Lazy<Boolean> isActiveActiveReplicationEnabledInAllRegion) {
    if (configValue != null && !isActiveActiveReplicationEnabledInAllRegion.get()) {
      LOGGER.info(
          "Ignoring config {} : {}, as store {} is not set up for Active/Active replication in all regions",
          configType,
          configValue,
          storeName);
      return null;
    }
    return configValue;
  }

  /**
   * Configures the source fabric to align with the native replication source fabric selection.
   * <p>
   * For incremental pushes using a real-time (RT) policy, the push job produces to the parent Kafka cluster.
   * In such cases, this method ensures that the source fabric is not overridden with the native replication (NR)
   * source fabric to maintain proper configuration.
   */
  protected void configureSourceFabric(
      Admin admin,
      Version version,
      Lazy<Boolean> isActiveActiveReplicationEnabledInAllRegions,
      RequestTopicForPushRequest request,
      VersionCreationResponse response) {
    PushType pushType = request.getPushType();
    // Handle native replication for non-incremental push types
    if (version.isNativeReplicationEnabled() && !pushType.isIncremental()) {
      String childDataCenterKafkaBootstrapServer = version.getPushStreamSourceAddress();
      if (childDataCenterKafkaBootstrapServer != null) {
        response.setKafkaBootstrapServers(childDataCenterKafkaBootstrapServer);
      }
      response.setKafkaSourceRegion(version.getNativeReplicationSourceFabric());
    }

    // Handle incremental push with override for source region
    if (admin.isParent() && pushType.isIncremental()) {
      overrideSourceRegionAddressForIncrementalPushJob(
          admin,
          response,
          request.getClusterName(),
          request.getStoreName(),
          request.getEmergencySourceRegion(),
          request.getSourceGridFabric(),
          isActiveActiveReplicationEnabledInAllRegions.get(),
          version.isNativeReplicationEnabled());
      LOGGER.info(
          "Using source region: {} for incremental push job: {} on store: {} cluster: {}",
          response.getKafkaBootstrapServers(),
          request.getPushJobId(),
          request.getStoreName(),
          request.getClusterName());
    }
  }

  protected CompressionStrategy getCompressionStrategy(Version version, String responseTopic) {
    if (Version.isRealTimeTopic(responseTopic)) {
      return CompressionStrategy.NO_OP;
    }
    return version.getCompressionStrategy();
  }

  protected String determineResponseTopic(String storeName, Version version, RequestTopicForPushRequest request) {
    String responseTopic;
    PushType pushType = request.getPushType();
    if (pushType == PushType.INCREMENTAL) {
      // If incremental push with a dedicated real-time topic is enabled then use the separate real-time topic
      if (version.isSeparateRealTimeTopicEnabled() && request.isSeparateRealTimeTopicEnabled()) {
        responseTopic = Utils.getSeparateRealTimeTopicName(version);
      } else {
        responseTopic = Utils.getRealTimeTopicName(version);
      }
    } else if (pushType == PushType.STREAM) {
      responseTopic = Utils.getRealTimeTopicName(version);
    } else if (pushType == PushType.STREAM_REPROCESSING) {
      responseTopic = Version.composeStreamReprocessingTopic(storeName, version.getNumber());
    } else {
      responseTopic = version.kafkaTopicName();
    }
    return responseTopic;
  }

  protected void handleNonStreamPushType(
      Admin admin,
      Store store,
      RequestTopicForPushRequest request,
      VersionCreationResponse response,
      Lazy<Boolean> isActiveActiveReplicationEnabledInAllRegions) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    PushType pushType = request.getPushType();
    // Check if requestTopicForPush can be handled by child controllers for the given store
    if (!admin.whetherEnableBatchPushFromAdmin(clusterName, storeName)) {
      throw new VeniceUnsupportedOperationException(
          request.getPushType().name(),
          "Please push data to Venice Parent Colo instead");
    }
    int computedPartitionCount = admin.calculateNumberOfPartitions(clusterName, storeName);
    final Version version = admin.incrementVersionIdempotent(
        clusterName,
        storeName,
        request.getPushJobId(),
        computedPartitionCount,
        response.getReplicas(),
        pushType,
        request.isSendStartOfPush(),
        request.isSorted(),
        request.getCompressionDictionary(),
        Optional.ofNullable(request.getSourceGridFabric()),
        Optional.ofNullable(request.getCertificateInRequest()),
        request.getRewindTimeInSecondsOverride(),
        Optional.ofNullable(request.getEmergencySourceRegion()),
        request.isDeferVersionSwap(),
        request.getTargetedRegions(),
        request.getRepushSourceVersion(),
        request.getRepushTtlSeconds());

    // Set the partition count
    response.setPartitions(version.getPartitionCount());
    // Set the version number
    response.setVersion(version.getNumber());
    // Set the response topic
    response.setKafkaTopic(determineResponseTopic(storeName, version, request));
    // Set the compression strategy
    response.setCompressionStrategy(getCompressionStrategy(version, response.getKafkaTopic()));
    // Set the bootstrap servers
    configureSourceFabric(admin, version, isActiveActiveReplicationEnabledInAllRegions, request, response);
  }

  /**
   * Method handle request to get a topic for pushing data to Venice with {@link PushType#STREAM}
   */
  protected void handleStreamPushType(
      Admin admin,
      Store store,
      RequestTopicForPushRequest request,
      VersionCreationResponse response) {
    if (admin.isParent()) {
      throw new VeniceException(
          "Write operations to the parent region are not permitted with push type: STREAM, as this feature is currently disabled.");
    }

    Version referenceHybridVersion = admin.getReferenceVersionForStreamingWrites(
        request.getClusterName(),
        request.getStoreName(),
        request.getPushJobId());
    if (referenceHybridVersion == null) {
      LOGGER.error(
          "Request to get topic for STREAM push: {} for store: {} in cluster: {} is rejected as no hybrid version found",
          request.getPushJobId(),
          store.getName(),
          request.getClusterName());
      throw new VeniceException(
          "No hybrid version found for store: " + store.getName() + " in cluster: " + request.getClusterName()
              + ". Create a hybrid version before starting a stream push job.");
    }
    response.setPartitions(referenceHybridVersion.getPartitionCount());
    response.setCompressionStrategy(CompressionStrategy.NO_OP);
    response.setKafkaTopic(Utils.getRealTimeTopicName(referenceHybridVersion));

    // Override Kafka bootstrap servers if source grid fabric is set and feature is enabled
    String sourceGridFabric = request.getSourceGridFabric();
    if (sourceGridFabric != null
        && admin.getControllerConfig(request.getClusterName()).isEnableStreamPushSourceGridFabricOverride()
        && referenceHybridVersion.isActiveActiveReplicationEnabled()) {
      String bootstrapServerAddress = admin.getPubSubBootstrapServersForRegion(sourceGridFabric);
      if (bootstrapServerAddress == null) {
        LOGGER.error(
            "Failed to get the broker server URL for source grid fabric: {} for pushJob: {} store: {} in cluster: {}. Will use default PubSub bootstrap servers.",
            sourceGridFabric,
            request.getPushJobId(),
            store.getName(),
            request.getClusterName());
      } else {
        LOGGER.info(
            "Stream push job source region is being overridden with: {} address: {} for pushJob: {} on store: {} in cluster: {}",
            sourceGridFabric,
            bootstrapServerAddress,
            request.getPushJobId(),
            store.getName(),
            request.getClusterName());
        response.setKafkaBootstrapServers(bootstrapServerAddress);
      }
    }
  }

  /**
   * This method is used to handle the request to get a topic for pushing data to Venice.
   */
  void handleRequestTopicForPushing(Admin admin, RequestTopicForPushRequest request, VersionCreationResponse response) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    response.setCluster(clusterName);
    response.setName(storeName);

    // Check if the store exists
    Store store = admin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }

    // Verify and configure the partitioner
    verifyAndConfigurePartitionerSettings(store.getPartitionerConfig(), request.getPartitioners(), response);

    // Validate push type
    validatePushType(request.getPushType(), store);

    // Create aa replication checks with lazy evaluation
    Lazy<Boolean> isActiveActiveReplicationEnabledInAllRegions =
        getActiveActiveReplicationCheck(admin, store, clusterName, storeName, false);

    // Validate source and emergency region details and update request object
    String sourceGridFabric = applyConfigBasedOnReplication(
        SOURCE_GRID_FABRIC,
        request.getSourceGridFabric(),
        storeName,
        isActiveActiveReplicationEnabledInAllRegions);
    String emergencySourceRegion = applyConfigBasedOnReplication(
        EMERGENCY_SOURCE_REGION,
        admin.getEmergencySourceRegion(clusterName).orElse(null),
        storeName,
        isActiveActiveReplicationEnabledInAllRegions);

    request.setSourceGridFabric(sourceGridFabric);
    request.setEmergencySourceRegion(emergencySourceRegion);
    LOGGER.info(
        "Request to push to store: {} in cluster: {} with source grid fabric: {} and emergency source region: {}",
        storeName,
        clusterName,
        sourceGridFabric != null ? sourceGridFabric : "N/A",
        emergencySourceRegion != null ? emergencySourceRegion : "N/A");

    // Set the store's replication factor and partition count
    response.setReplicas(admin.getReplicationFactor(clusterName, storeName));

    boolean isSSL = admin.isSSLEnabledForPush(clusterName, storeName);
    response.setKafkaBootstrapServers(admin.getKafkaBootstrapServers(isSSL));
    response.setKafkaSourceRegion(admin.getRegionName());
    response.setEnableSSL(isSSL);

    PushType pushType = request.getPushType();
    if (pushType == PushType.STREAM) {
      handleStreamPushType(admin, store, request, response);
    } else {
      handleNonStreamPushType(admin, store, request, response, isActiveActiveReplicationEnabledInAllRegions);
    }

    response.setDaVinciPushStatusStoreEnabled(store.isDaVinciPushStatusStoreEnabled());
    response.setAmplificationFactor(1);
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
        if (!isAllowListUser(request)) {
          if (!hasWriteAccessToTopic(request)) {
            buildStoreAclErrorAndThrowException(request, response, true, false);
          }

          if (this.checkReadMethodForKafka && !hasReadAccessToTopic(request)) {
            buildStoreAclErrorAndThrowException(request, response, false, true);
          }
        }

        // Validate the request parameters
        AdminSparkServer.validateParams(request, REQUEST_TOPIC.getParams(), admin);

        // Extract request parameters and create a RequestTopicForPushRequest object
        RequestTopicForPushRequest requestTopicForPushRequest = new RequestTopicForPushRequest(
            request.queryParams(CLUSTER),
            request.queryParams(NAME),
            PushType.extractPushType(request.queryParams(PUSH_TYPE)),
            request.queryParams(PUSH_JOB_ID));

        // populate the request object with optional parameters
        extractOptionalParamsFromRequestTopicRequest(request, requestTopicForPushRequest, isAclEnabled());

        // Invoke the handler to get the topic for pushing data
        handleRequestTopicForPushing(admin, requestTopicForPushRequest, responseObject);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }

      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * When partners have ACL issues for their push, we should provide an accurate and informative messages that
   * help partners to unblock by themselves.
   */
  private void buildStoreAclErrorAndThrowException(
      Request request,
      Response response,
      boolean missingWriteAccess,
      boolean missingReadAccess) throws JsonProcessingException {
    response.status(HttpStatus.SC_FORBIDDEN);
    String userId = getPrincipalId(request);
    String errorMessage = "Missing [%s] ACLs for user \"" + userId + "\". Please setup ACLs for your store.";
    if (missingWriteAccess) {
      errorMessage = String.format(errorMessage, "write");
    } else if (missingReadAccess) {
      errorMessage = String.format(errorMessage, "read");
    } else {
      errorMessage = String.format(errorMessage, "read and write");
    }

    throw new VeniceStoreAclException(errorMessage);
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
      String storeName,
      String emergencySourceRegion,
      String pushJobSourceGridFabric,
      boolean isAAEnabledInAllRegions,
      boolean isNativeReplicationEnabled) {
    if (!isAAEnabledInAllRegions && isNativeReplicationEnabled) {
      // P2: When AA is not enabled in all the regions we use aggregate RT address, if it is available,
      // for inc-pushes if native-replication is enabled.
      Optional<String> aggregateRealTimeTopicSource = admin.getAggregateRealTimeTopicSource(clusterName);
      if (aggregateRealTimeTopicSource.isPresent()) {
        response.setKafkaBootstrapServers(aggregateRealTimeTopicSource.get());
        LOGGER.info(
            "Incremental push job source region is being overridden with: {} address: {} for store: {} in cluster: {}",
            aggregateRealTimeTopicSource.get(),
            response.getKafkaBootstrapServers(),
            storeName,
            clusterName);
      }

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
    String bootstrapServerAddress = admin.getPubSubBootstrapServersForRegion(overRideSourceRegion);
    if (bootstrapServerAddress == null) {
      throw new VeniceException("Failed to get the broker server URL for the source region: " + overRideSourceRegion);
    }
    LOGGER.info(
        "Incremental push job source region is being overridden with: {} address: {} for store: {} in cluster: {}",
        overRideSourceRegion,
        bootstrapServerAddress,
        storeName,
        clusterName);
    response.setKafkaBootstrapServers(bootstrapServerAddress);
  }

  static void validatePushType(PushType pushType, Store store) {
    if (pushType.equals(PushType.STREAM) && !store.isHybrid()) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "requesting topic for streaming writes to store " + store.getName()
              + " which is not configured to be a hybrid store",
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
            -1,
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
        clusterName = request.queryParams(CLUSTER);
        if (!admin.whetherEnableBatchPushFromAdmin(clusterName, storeName)) {
          throw new VeniceUnsupportedOperationException(
              "EMPTY PUSH",
              "Please push data to Venice Parent Colo instead or use Aggregate mode if you are running Samza GF Job.");
        }

        String pushJobId = request.queryParams(PUSH_JOB_ID);
        int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName);
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);

        Store store = admin.getStore(clusterName, storeName);
        if (store == null) {
          LOGGER.error(
              "Request to empty push with job id: {} for store: {} in cluster: {} is rejected as no store found",
              pushJobId,
              storeName,
              clusterName);
          throw new VeniceNoStoreException(storeName, clusterName);
        }
        Set<Version> previousVersions = new HashSet<>(store.getVersions());
        boolean isDeferredVersionSwapForEmptyPushEnabled = admin.isDeferredVersionSwapForEmptyPushEnabled(storeName);
        if (isDeferredVersionSwapForEmptyPushEnabled) {
          String targetRegion = admin.getDeferredVersionSwapRegionRollforwardOrder(storeName);
          version = admin.incrementVersionIdempotent(
              clusterName,
              storeName,
              pushJobId,
              partitionNum,
              replicationFactor,
              Version.PushType.BATCH,
              true,
              false,
              null,
              Optional.empty(),
              Optional.empty(),
              -1,
              Optional.empty(),
              true,
              targetRegion,
              -1,
              -1);
        } else {
          version =
              admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionNum, replicationFactor);
        }

        int versionNumber = version.getNumber();
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
        responseObject.setPartitions(partitionNum);
        responseObject.setReplicas(replicationFactor);
        responseObject.setKafkaTopic(version.kafkaTopicName());
        responseObject.setKafkaBootstrapServers(version.getPushStreamSourceAddress());

        if (!previousVersions.contains(version)) {
          LOGGER.info(
              "Sending SOP and EOP for empty push job: {} for store: {} in cluster: {}",
              pushJobId,
              storeName,
              clusterName);
          admin.writeEndOfPush(clusterName, storeName, versionNumber, true);
        } else {
          LOGGER.info(
              "Empty push job: {} for store: {} in cluster: {} is a duplicate empty push. No new version was created and no SOP/EOP was sent again.",
              pushJobId,
              storeName,
              clusterName);
        }

        /** TODO: Poll {@link com.linkedin.venice.controller.VeniceParentHelixAdmin#getOffLineJobStatus(String, String, Map, TopicManager)} until it is terminal... */

      } catch (Throwable e) {
        // Clean up on failed push.
        if (version != null && clusterName != null) {
          LOGGER.warn("Cleaning up failed Empty push of {}", version.kafkaTopicName());
          admin.killOfflinePush(
              clusterName,
              version.kafkaTopicName(),
              PushJobKillTrigger.VERSION_CREATION_FAILURE,
              e.getMessage(),
              true);
        }
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
