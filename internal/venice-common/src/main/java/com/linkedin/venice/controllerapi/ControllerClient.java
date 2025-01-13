package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACCESS_PERMISSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.AMPLIFICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BATCH_JOB_HEARTBEAT_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER_DEST;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.COMPRESSION_DICTIONARY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DATA_RECOVERY_COPY_ALL_VERSION_CONFIGS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DEFER_VERSION_SWAP;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DERIVED_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DERIVED_SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DEST_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_DISABLED_REPLICAS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXECUTION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXPECTED_ROUTER_COUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC_A;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FABRIC_B;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HEARTBEAT_TIMESTAMP;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCLUDE_SYSTEM_STORES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCREMENTAL_PUSH_VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.IS_SYSTEM_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.IS_WRITE_COMPUTE_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KAFKA_TOPIC_LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KAFKA_TOPIC_MIN_IN_SYNC_REPLICA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KAFKA_TOPIC_RETENTION_IN_MS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OFFSET;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OPERATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONERS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITION_COUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITION_DETAIL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PERSONA_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PERSONA_OWNERS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PERSONA_QUOTA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PERSONA_STORES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_IN_SORTED_ORDER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_JOB_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_TYPE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_OPERATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_WRITE_OPERATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REGIONS_FILTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REMOTE_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_METADATA_VERSION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPUSH_SOURCE_VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SCHEMA_COMPAT_TYPE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEND_START_OF_PUSH;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEPARATE_REAL_TIME_TOPIC_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SKIP_DIV;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SOURCE_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SOURCE_FABRIC_VERSION_INCLUDED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STATUS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_NODE_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_CONFIG_NAME_FILTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_CONFIG_VALUE_FILTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_SIZE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_TYPE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGETED_REGIONS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TOPIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TO_BE_STOPPED_INSTANCES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.UPSTREAM_OFFSET;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VOLDEMORT_STORE_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.WRITE_OPERATION;
import static com.linkedin.venice.meta.Version.PushType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ControllerClient implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(ControllerClient.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private static final int DEFAULT_MAX_ATTEMPTS = 10;
  private static final int QUERY_JOB_STATUS_TIMEOUT = 60 * Time.MS_PER_SECOND;
  private static final int DEFAULT_REQUEST_TIMEOUT_MS = 600 * Time.MS_PER_SECOND;
  private final Optional<SSLFactory> sslFactory;
  private final String clusterName;
  private final VeniceJsonSerializer<Version> versionVeniceJsonSerializer = new VeniceJsonSerializer<>(Version.class);
  private String leaderControllerUrl;
  private final List<String> controllerDiscoveryUrls;

  public ControllerClient(String clusterName, String discoveryUrls) {
    this(clusterName, discoveryUrls, Optional.empty());
  }

  /**
   * @param discoveryUrls comma-delimited urls to find leader controller.
   */
  public ControllerClient(String clusterName, String discoveryUrls, Optional<SSLFactory> sslFactory) {
    if (StringUtils.isEmpty(discoveryUrls)) {
      throw new VeniceException("Controller discovery url list is empty: " + discoveryUrls);
    }

    this.sslFactory = sslFactory;
    this.clusterName = clusterName;
    this.controllerDiscoveryUrls =
        Arrays.stream(discoveryUrls.split(",")).map(String::trim).collect(Collectors.toList());
    if (this.controllerDiscoveryUrls.isEmpty()) {
      throw new VeniceException("Controller discovery url list is empty");
    }
  }

  /**
   * @deprecated Use {@link ControllerClientFactory#discoverAndConstructControllerClient}
   */
  @Deprecated
  public static ControllerClient discoverAndConstructControllerClient(
      String storeName,
      String discoveryUrls,
      Optional<SSLFactory> sslFactory,
      int retryAttempts) {
    return ControllerClientFactory
        .discoverAndConstructControllerClient(storeName, discoveryUrls, sslFactory, retryAttempts);
  }

  /**
   * @deprecated Use {@link ControllerClientFactory#getControllerClient}
   */
  @Deprecated
  public static ControllerClient constructClusterControllerClient(String clusterName, String discoveryUrls) {
    return constructClusterControllerClient(clusterName, discoveryUrls, Optional.empty());
  }

  /**
   * @deprecated Use {@link ControllerClientFactory#getControllerClient}
   */
  @Deprecated
  public static ControllerClient constructClusterControllerClient(
      String clusterName,
      String discoveryUrls,
      Optional<SSLFactory> sslFactory) {
    return ControllerClientFactory.getControllerClient(clusterName, discoveryUrls, sslFactory);
  }

  @Override
  public void close() {
    // Currently, we do not have any resources to clean up. If there is something to clean-up, we must check the return
    // value of "ControllerClientFactory.release(this)" and if that is true, only then is it safe to clean up resources.
    // if (ControllerClientFactory.release(this)) {
    // // Object is no longer used in other places. Safe to clean up resources
    // ...
    // }
    ControllerClientFactory.release(this);
  }

  protected String discoverLeaderController() {
    List<String> urls = new ArrayList<>(this.controllerDiscoveryUrls);
    Collections.shuffle(urls);

    Exception lastConnectException = null;
    Exception lastException = null;
    try (ControllerTransport transport = new ControllerTransport(sslFactory)) {
      for (String url: urls) {
        try {
          String leaderControllerUrl =
              transport.request(url, ControllerRoute.LEADER_CONTROLLER, newParams(), LeaderControllerResponse.class)
                  .getUrl();
          LOGGER.info("Discovered leader controller {} from {}", leaderControllerUrl, url);
          return leaderControllerUrl;
        } catch (Exception e) {
          LOGGER.warn("Unable to discover leader controller from {}", url);
          if (ExceptionUtils.recursiveClassEquals(e, ConnectException.class)) {
            lastConnectException = e;
          } else {
            lastException = e;
          }
        }
      }
    }

    /**
     * During normal operation, some hosts might be down for maintenance. Over time, the host list might even get
     * stale. So, when requests are made to hosts which no longer host venice controllers or routers,
     * {@link ConnectException} will be thrown. When the request actually leads to an error, all active hosts in
     * the host list will throw this error and the inactive hosts will throw the {@link ConnectException}. In such
     * cases, the {@link ConnectException} is not actionable by the user. If after trying all controllers and routers,
     * we still don't have any other non-{@link ConnectException}, the {@link ConnectException} is the most actionable
     * exception, and we throw that.
     */
    if (lastException == null) {
      lastException = lastConnectException;
    }

    String message = "Unable to discover leader controller from " + this.controllerDiscoveryUrls;
    LOGGER.error(message, lastException);
    throw new VeniceException(message, lastException);
  }

  public StoreResponse getStore(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.STORE, params, StoreResponse.class);
  }

  public StoreResponse getStore(String storeName, int timeoutMs) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.STORE, params, StoreResponse.class, timeoutMs, 1, null);
  }

  public RepushInfoResponse getRepushInfo(String storeName, Optional<String> fabricName) {
    QueryParams params = newParams().add(NAME, storeName);
    fabricName.ifPresent(s -> params.add(FABRIC, s));
    return request(ControllerRoute.GET_REPUSH_INFO, params, RepushInfoResponse.class);
  }

  public MultiStoreStatusResponse getFutureVersions(String clusterName, String storeName) {
    QueryParams params = newParams().add(NAME, storeName).add(CLUSTER, clusterName);
    return request(ControllerRoute.FUTURE_VERSION, params, MultiStoreStatusResponse.class);
  }

  public MultiStoreStatusResponse getBackupVersions(String clusterName, String storeName) {
    QueryParams params = newParams().add(NAME, storeName).add(CLUSTER, clusterName);
    return request(ControllerRoute.BACKUP_VERSION, params, MultiStoreStatusResponse.class);
  }

  @Deprecated
  public static StoreResponse getStore(String urlsToFindLeaderController, String clusterName, String storeName) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindLeaderController)) {
      return client.getStore(storeName);
    }
  }

  public StorageEngineOverheadRatioResponse getStorageEngineOverheadRatio(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.STORAGE_ENGINE_OVERHEAD_RATIO, params, StorageEngineOverheadRatioResponse.class);
  }

  public SchemaUsageResponse getInUseSchemaIds(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.GET_INUSE_SCHEMA_IDS, params, SchemaUsageResponse.class);
  }

  public ControllerResponse deleteValueSchemas(String storeName, List<String> schemaIds) {
    QueryParams params =
        newParams().add(NAME, storeName).add(ControllerApiConstants.VALUE_SCHEMA_IDS, String.join(",", schemaIds));
    return request(ControllerRoute.DELETE_UNUSED_VALUE_SCHEMAS, params, SchemaUsageResponse.class);
  }

  public VersionCreationResponse requestTopicForWrites(
      String storeName,
      long storeSize,
      PushType pushType,
      String pushJobId,
      boolean sendStartOfPush,
      boolean sorted,
      boolean wcEnabled,
      Optional<String> partitioners,
      Optional<String> compressionDictionary,
      Optional<String> sourceGridFabric,
      boolean batchJobHeartbeatEnabled,
      long rewindTimeInSecondsOverride) {
    return requestTopicForWrites(
        storeName,
        storeSize,
        pushType,
        pushJobId,
        sendStartOfPush,
        sorted,
        wcEnabled,
        partitioners,
        compressionDictionary,
        sourceGridFabric,
        batchJobHeartbeatEnabled,
        rewindTimeInSecondsOverride,
        false,
        null,
        -1,
        false);
  }

  public VersionCreationResponse requestTopicForWrites(
      String storeName,
      long storeSize,
      PushType pushType,
      String pushJobId,
      boolean sendStartOfPush,
      boolean sorted,
      boolean wcEnabled,
      Optional<String> partitioners,
      Optional<String> compressionDictionary,
      Optional<String> sourceGridFabric,
      boolean batchJobHeartbeatEnabled,
      long rewindTimeInSecondsOverride,
      boolean deferVersionSwap) {
    return requestTopicForWrites(
        storeName,
        storeSize,
        pushType,
        pushJobId,
        sendStartOfPush,
        sorted,
        wcEnabled,
        partitioners,
        compressionDictionary,
        sourceGridFabric,
        batchJobHeartbeatEnabled,
        rewindTimeInSecondsOverride,
        deferVersionSwap,
        null,
        -1,
        false);
  }

  /**
   * Request a topic for the VeniceWriter to write into.  A new VPJ push, or a Samza bulk processing job should both use
   * this method.  The push job ID needs to be unique for this push.  Multiple requests with the same pushJobId are
   * idempotent and will return the same topic.
   *
   * @param storeName                   Name of the store being written to.
   * @param storeSize                   Estimated size of push in bytes, used to determine partitioning
   * @param pushJobId                   Unique identifier for this job
   * @param sendStartOfPush             Whether controller should send START_OF_PUSH message to the newly created topic,
   *                                    while adding a new version. This is currently used in Samza batch load, a.k.a.
   *                                    grandfather
   * @param sorted                      Whether the push is going to contain sorted data (in each partition) or not
   * @param wcEnabled                   Whether write compute is enabled for this push job or not
   * @param partitioners                partitioner class names in a string seperated by comma
   * @param compressionDictionary       Base64 encoded dictionary to be used to perform dictionary compression
   * @param sourceGridFabric            An identifier of the data center which is used in native replication to
   *                                    determine the Kafka URL
   * @param batchJobHeartbeatEnabled    whether batch push job enables the heartbeat
   * @param rewindTimeInSecondsOverride if a valid value is specified (>=0) for hybrid store, this param will override
   *                                    the default store-level rewindTimeInSeconds config.
   * @param deferVersionSwap            whether to defer version swap after the push is done
   * @param targetedRegions             the list of regions that is separated by comma for targeted region push.
   * @param repushSourceVersion
   * @return VersionCreationResponse includes topic and partitioning
   */
  public VersionCreationResponse requestTopicForWrites(
      String storeName,
      long storeSize,
      PushType pushType,
      String pushJobId,
      boolean sendStartOfPush,
      boolean sorted,
      boolean wcEnabled,
      Optional<String> partitioners,
      Optional<String> compressionDictionary,
      Optional<String> sourceGridFabric,
      boolean batchJobHeartbeatEnabled,
      long rewindTimeInSecondsOverride,
      boolean deferVersionSwap,
      String targetedRegions,
      int repushSourceVersion,
      boolean pushToSeparateRealtimeTopic) {
    QueryParams params = newParams().add(NAME, storeName)
        // TODO: Store size is not used anymore. Remove it after the next round of controller deployment.
        .add(STORE_SIZE, Long.toString(storeSize))
        .add(PUSH_JOB_ID, pushJobId)
        .add(PUSH_TYPE, pushType.toString())
        .add(SEND_START_OF_PUSH, sendStartOfPush)
        .add(PUSH_IN_SORTED_ORDER, sorted)
        .add(IS_WRITE_COMPUTE_ENABLED, wcEnabled)
        .add(PARTITIONERS, partitioners)
        .add(COMPRESSION_DICTIONARY, compressionDictionary)
        .add(SOURCE_GRID_FABRIC, sourceGridFabric)
        .add(BATCH_JOB_HEARTBEAT_ENABLED, batchJobHeartbeatEnabled)
        .add(REWIND_TIME_IN_SECONDS_OVERRIDE, rewindTimeInSecondsOverride)
        .add(DEFER_VERSION_SWAP, deferVersionSwap)
        .add(REPUSH_SOURCE_VERSION, repushSourceVersion)
        .add(SEPARATE_REAL_TIME_TOPIC_ENABLED, pushToSeparateRealtimeTopic);
    if (StringUtils.isNotEmpty(targetedRegions)) {
      params.add(TARGETED_REGIONS, targetedRegions);
    }

    return request(ControllerRoute.REQUEST_TOPIC, params, VersionCreationResponse.class);
  }

  /**
   * Used for store migration to add version and start ingestion in destination cluster for new pushes in the source
   * cluster during the ingestion. The idea is like copying or create a version on an existing topic. Different use
   * cases can be explored and expanded in the future. Applicable only to child controllers.
   * @param storeName of the original push.
   * @param pushJobId of the original push.
   * @param version of the original push.
   * @param partitionCount of the original push.
   * @param pushType of the producer.
   * @param remoteKafkaBootstrapServers of the original push.
   * @param rewindTimeInSecondsOverride of the original push.
   * @param replicationMetadataVersionId of the original push.
   * @return
   */
  public VersionResponse addVersionAndStartIngestion(
      String storeName,
      String pushJobId,
      int version,
      int partitionCount,
      Version.PushType pushType,
      String remoteKafkaBootstrapServers,
      long rewindTimeInSecondsOverride,
      int replicationMetadataVersionId) {
    QueryParams params = newParams().add(NAME, storeName)
        .add(PUSH_JOB_ID, pushJobId)
        .add(VERSION, version)
        .add(PARTITION_COUNT, partitionCount)
        .add(PUSH_TYPE, pushType.toString())
        .add(REWIND_TIME_IN_SECONDS_OVERRIDE, rewindTimeInSecondsOverride)
        .add(REPLICATION_METADATA_VERSION_ID, replicationMetadataVersionId);
    if (remoteKafkaBootstrapServers != null) {
      params.add(REMOTE_KAFKA_BOOTSTRAP_SERVERS, remoteKafkaBootstrapServers);
    }
    return request(ControllerRoute.ADD_VERSION, params, VersionResponse.class);
  }

  public ControllerResponse writeEndOfPush(String storeName, int version) {
    QueryParams params = newParams().add(NAME, storeName).add(VERSION, version);
    return request(ControllerRoute.END_OF_PUSH, params, ControllerResponse.class);
  }

  /**
   * Sends and empty push to the venice controller, but verifies that the push has succeeded before
   * returning to the caller.
   *
   * @param storeName the store name for which the empty push is for
   * @param pushJobId the push job id for the push
   * @param storeSize the size of the store (currently unused)
   * @param timeOut max amount of time this function should take before returning in MILLISECONDS.  Retries sent to the controller
   *                have 2-second sleeps between them.  So a timeout should be chosen that is larger, and a multiple of
   *                2 seconds preferably.
   * @return the response from the controller.  Either a successful one, or a failed one with more information.
   */
  public ControllerResponse sendEmptyPushAndWait(String storeName, String pushJobId, long storeSize, long timeOut) {
    // Check Store existence
    VersionCreationResponse versionCreationResponse = emptyPush(storeName, pushJobId, storeSize);
    if (versionCreationResponse.isError()) {
      return versionCreationResponse;
    }
    String topicName = Version.composeKafkaTopic(storeName, versionCreationResponse.getVersion());
    try {
      long startTime = System.currentTimeMillis();
      long endTime = startTime + timeOut;
      JobStatusQueryResponse jobStatusQueryResponse;
      while (true) {
        jobStatusQueryResponse = retryableRequest(3, client -> this.queryJobStatus(topicName));
        if (jobStatusQueryResponse.isError()) {
          return jobStatusQueryResponse;
        }
        ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatusQueryResponse.getStatus());
        if (executionStatus.isTerminal()) {
          return jobStatusQueryResponse;
        }
        if (System.currentTimeMillis() > endTime) {
          throw new VeniceException(
              "sendEmptyPushAndWait did not succeed in the allotted time (" + timeOut + " ms). Last status: "
                  + jobStatusQueryResponse.toString());
        }
        Thread.sleep(1 * Time.MS_PER_SECOND);
      }
    } catch (VeniceException e) {
      throw e;
    } catch (Exception e) {
      throw new VeniceException("Caught Exception while trying to send empty push.", e);
    }
  }

  // TODO: Refactor this to work in the controller once system store has become available.

  /**
   * Simplified API that wraps together the store create and update functionalities with some clean up functionality
   *
   * @param storeName the store name to create and update
   * @param owner the owner of this store to be created
   * @param keySchema Schema of the key for row retrieval for this store
   * @param valueSchema Schema of the value for rows in this new store
   * @param updateStoreQueryParams What parameters should be applied to this store after it's creation
   * @return The response from the first failed operation of store creation or modification
   */
  public ControllerResponse createNewStoreWithParameters(
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      UpdateStoreQueryParams updateStoreQueryParams) {
    NewStoreResponse creationResponse = this.createNewStore(storeName, owner, keySchema, valueSchema);
    if (creationResponse.isError()) {
      // Return the error
      return creationResponse;
    }

    ControllerResponse updateResponse = null;
    try {
      updateResponse = updateStore(storeName, updateStoreQueryParams);
      if (updateResponse.isError()) {
        // update failed. Let's clean up and return the error
        if (!this.getStore(storeName).isError()) {
          return updateResponse;
        }
      }
    } finally {
      if (creationResponse == null || updateResponse == null) {
        // If any step in this process failed (that is, the store was created in some inconsistent state), clean up.
        if (!this.getStore(storeName).isError()) {
          this.disableAndDeleteStore(storeName);
        }
      }
    }
    return creationResponse;
  }

  public VersionCreationResponse emptyPush(String storeName, String pushJobId, long storeSize) {
    // TODO: Store size is not used anymore. Remove it after the next round of controller deployment.
    QueryParams params =
        newParams().add(NAME, storeName).add(PUSH_JOB_ID, pushJobId).add(STORE_SIZE, Long.toString(storeSize));
    return request(ControllerRoute.EMPTY_PUSH, params, VersionCreationResponse.class);
  }

  public NewStoreResponse createNewStore(String storeName, String owner, String keySchema, String valueSchema) {
    QueryParams params =
        newParams().add(NAME, storeName).add(OWNER, owner).add(KEY_SCHEMA, keySchema).add(VALUE_SCHEMA, valueSchema);
    return request(ControllerRoute.NEW_STORE, params, NewStoreResponse.class);
  }

  public NewStoreResponse createNewStore(
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      String accessPermissions) {
    QueryParams params = newParams().add(NAME, storeName)
        .add(OWNER, owner)
        .add(KEY_SCHEMA, keySchema)
        .add(VALUE_SCHEMA, valueSchema)
        .add(ACCESS_PERMISSION, accessPermissions);
    return request(ControllerRoute.NEW_STORE, params, NewStoreResponse.class);
  }

  public NewStoreResponse createNewSystemStore(String storeName, String owner, String keySchema, String valueSchema) {
    QueryParams params = newParams().add(NAME, storeName)
        .add(OWNER, owner)
        .add(KEY_SCHEMA, keySchema)
        .add(VALUE_SCHEMA, valueSchema)
        .add(IS_SYSTEM_STORE, true);
    return request(ControllerRoute.NEW_STORE, params, NewStoreResponse.class);
  }

  public StoreMigrationResponse isStoreMigrationAllowed() {
    return request(ControllerRoute.STORE_MIGRATION_ALLOWED, newParams(), StoreMigrationResponse.class);
  }

  public StoreMigrationResponse migrateStore(String storeName, String destClusterName) {
    QueryParams params = newParams().add(NAME, storeName).add(CLUSTER_DEST, destClusterName);
    return request(ControllerRoute.MIGRATE_STORE, params, StoreMigrationResponse.class);
  }

  public StoreMigrationResponse completeMigration(String storeName, String destClusterName) {
    QueryParams params = newParams().add(NAME, storeName).add(CLUSTER_DEST, destClusterName);
    return request(ControllerRoute.COMPLETE_MIGRATION, params, StoreMigrationResponse.class);
  }

  /**
   * This command should be sent to src controller, not dest controller
   */
  public StoreMigrationResponse abortMigration(String storeName, String destClusterName) {
    QueryParams params = newParams().add(NAME, storeName).add(CLUSTER_DEST, destClusterName);
    return request(ControllerRoute.ABORT_MIGRATION, params, StoreMigrationResponse.class);
  }

  public TrackableControllerResponse deleteStore(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.DELETE_STORE, params, TrackableControllerResponse.class);
  }

  public ControllerResponse wipeCluster(String fabric, Optional<String> storeName, Optional<Integer> versionNum) {
    QueryParams params = newParams().add(FABRIC, fabric);
    storeName.ifPresent(s -> params.add(NAME, s));
    versionNum.ifPresent(v -> params.add(VERSION, v));
    return request(ControllerRoute.WIPE_CLUSTER, params, ControllerResponse.class);
  }

  public StoreComparisonResponse compareStore(String storeName, String fabricA, String fabricB) {
    QueryParams params = newParams().add(NAME, storeName).add(FABRIC_A, fabricA).add(FABRIC_B, fabricB);
    return request(ControllerRoute.COMPARE_STORE, params, StoreComparisonResponse.class);
  }

  public StoreResponse copyOverStoreMetadata(String sourceFabric, String destFabric, String storeName) {
    QueryParams params = newParams().add(SOURCE_FABRIC, sourceFabric).add(DEST_FABRIC, destFabric).add(NAME, storeName);
    return request(ControllerRoute.REPLICATE_META_DATA, params, StoreResponse.class);
  }

  public ControllerResponse disableAndDeleteStore(String storeName) {
    UpdateStoreQueryParams updateParams = new UpdateStoreQueryParams().setEnableWrites(false).setEnableReads(false);
    ControllerResponse response = updateStore(storeName, updateParams);
    if (!response.isError()) {
      response = deleteStore(storeName);
    }
    return response;
  }

  public VersionResponse overrideSetActiveVersion(String storeName, int version) {
    QueryParams params = newParams().add(NAME, storeName).add(VERSION, version);
    return request(ControllerRoute.SET_VERSION, params, VersionResponse.class);
  }

  public ControllerResponse rollbackToBackupVersion(String storeName, String regionFilter) {
    QueryParams params = newParams().add(NAME, storeName).add(REGIONS_FILTER, regionFilter);
    return request(ControllerRoute.ROLLBACK_TO_BACKUP_VERSION, params, ControllerResponse.class);
  }

  public ControllerResponse rollbackToBackupVersion(String storeName) {
    return rollbackToBackupVersion(storeName, "");
  }

  public ControllerResponse rollForwardToFutureVersion(String storeName, String regionFilter) {
    QueryParams params = newParams().add(NAME, storeName).add(REGIONS_FILTER, regionFilter);
    return request(ControllerRoute.ROLL_FORWARD_TO_FUTURE_VERSION, params, ControllerResponse.class);
  }

  public ControllerResponse rollForwardToFutureVersion(String storeName) {
    return rollForwardToFutureVersion(storeName, "");
  }

  public ControllerResponse killOfflinePushJob(String kafkaTopic) {
    String store = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int versionNumber = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    QueryParams params = newParams().add(TOPIC, kafkaTopic) // TODO: remove once the controller is deployed to handle
                                                            // store and version instead
        .add(NAME, store)
        .add(VERSION, versionNumber);
    return request(ControllerRoute.KILL_OFFLINE_PUSH_JOB, params, ControllerResponse.class);
  }

  public ControllerResponse skipAdminMessage(String offset, boolean skipDIV) {
    QueryParams params = newParams().add(OFFSET, offset).add(SKIP_DIV, skipDIV);
    return request(ControllerRoute.SKIP_ADMIN, params, ControllerResponse.class);
  }

  public PubSubTopicConfigResponse getKafkaTopicConfigs(String kafkaTopicName) {
    QueryParams params = newParams().add(TOPIC, kafkaTopicName);
    return request(ControllerRoute.GET_KAFKA_TOPIC_CONFIGS, params, PubSubTopicConfigResponse.class);
  }

  public ControllerResponse updateKafkaTopicLogCompaction(String kafkaTopicName, boolean logCompactionEnabled) {
    QueryParams params =
        newParams().add(TOPIC, kafkaTopicName).add(KAFKA_TOPIC_LOG_COMPACTION_ENABLED, logCompactionEnabled);
    return request(ControllerRoute.UPDATE_KAFKA_TOPIC_LOG_COMPACTION, params, ControllerResponse.class);
  }

  public ControllerResponse updateKafkaTopicRetention(String kafkaTopicName, long retentionInMs) {
    QueryParams params = newParams().add(TOPIC, kafkaTopicName).add(KAFKA_TOPIC_RETENTION_IN_MS, retentionInMs);
    return request(ControllerRoute.UPDATE_KAFKA_TOPIC_RETENTION, params, ControllerResponse.class);
  }

  public ControllerResponse updateKafkaTopicMinInSyncReplica(String kafkaTopicName, int minISR) {
    QueryParams params = newParams().add(TOPIC, kafkaTopicName).add(KAFKA_TOPIC_MIN_IN_SYNC_REPLICA, minISR);
    return request(ControllerRoute.UPDATE_KAFKA_TOPIC_MIN_IN_SYNC_REPLICA, params, ControllerResponse.class);
  }

  public <R extends ControllerResponse> R retryableRequest(int totalAttempts, Function<ControllerClient, R> request) {
    return retryableRequest(this, totalAttempts, request, r -> false);
  }

  public <R extends ControllerResponse> R retryableRequest(
      int totalAttempts,
      Function<ControllerClient, R> request,
      Function<R, Boolean> abortRetryCondition) {
    return retryableRequest(this, totalAttempts, request, abortRetryCondition);
  }

  /**
   * Useful for pieces of code which want to have a test mocking the result of the function that's passed in...
   */
  public static <C extends ControllerClient, R extends ControllerResponse> R retryableRequest(
      C client,
      int totalAttempts,
      Function<C, R> request) {
    return retryableRequest(client, totalAttempts, request, r -> false);
  }

  public static <C extends ControllerClient, R extends ControllerResponse> R retryableRequest(
      C client,
      int totalAttempts,
      Function<C, R> request,
      Function<R, Boolean> abortRetryCondition) {
    if (totalAttempts < 1) {
      throw new VeniceException(
          "Querying with retries requires at least one attempt, called with " + totalAttempts + " attempts");
    }
    Exception exception = null;
    R response = null;

    for (int currentAttempt = 1; currentAttempt <= totalAttempts; currentAttempt++) {
      try {
        response = request.apply(client);
      } catch (Exception e) {
        exception = e;
      }
      // Do not retry if value schema is not found. TODO: Ideally response should not be an error but should return
      // INVALID schema ID in the response.
      if (exception == null && (!response.isError() || valueSchemaNotFoundSchemaResponse(response)
          || abortRetryCondition.apply(response))) {
        return response;
      } else {
        if (exception != null) {
          LOGGER
              .warn("Exception on attempt {}/{} of querying the Controller", currentAttempt, totalAttempts, exception);
        } else {
          LOGGER.warn(
              "Error on attempt {}/{} of querying the Controller: {}",
              currentAttempt,
              totalAttempts,
              response.getError());
        }
        Utils.sleep(2000);
      }
    }
    if (exception != null) {
      throw new VeniceException("Could not execute query even after " + totalAttempts + " attempts.", exception);
    } else {
      return response;
    }
  }

  private static <R> boolean valueSchemaNotFoundSchemaResponse(R response) {
    return (response instanceof SchemaResponse
        && ((SchemaResponse) response).getError().contains("Can not find any registered value schema for the store"));
  }

  /**
   * This method has a longer timeout intended to be used to query the overall job status on a parent controller. The
   * extended timeout is meant for the parent controller to query each colo's child controller for the job status and
   * aggregate the results. Use {@link ControllerClient#queryJobStatus(String, Optional)} instead if the target is a
   * child controller.
   * @param kafkaTopic, the version topic name of the push job.
   * @param incrementalPushVersion, the optional incremental push version of the push job.
   * @param targetedRegions, the list of regions that is separated by comma for targeted region push.
   * @return
   */
  public JobStatusQueryResponse queryOverallJobStatus(
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      String targetedRegions) {
    return queryJobStatus(kafkaTopic, incrementalPushVersion, 5 * QUERY_JOB_STATUS_TIMEOUT, targetedRegions);
  }

  public JobStatusQueryResponse queryOverallJobStatus(String kafkaTopic, Optional<String> incrementalPushVersion) {
    return queryOverallJobStatus(kafkaTopic, incrementalPushVersion, null);
  }

  public JobStatusQueryResponse queryJobStatus(String kafkaTopic) {
    return queryJobStatus(kafkaTopic, Optional.empty(), QUERY_JOB_STATUS_TIMEOUT, null);
  }

  /**
   * This method is used to query the job status from a controller. It is expected to be a child controller thus a
   * shorter timeout is enforced. Use {@link ControllerClient#queryOverallJobStatus(String, Optional)} instead if the
   * target is a parent controller.
   */
  public JobStatusQueryResponse queryJobStatus(String kafkaTopic, Optional<String> incrementalPushVersion) {
    return queryJobStatus(kafkaTopic, incrementalPushVersion, QUERY_JOB_STATUS_TIMEOUT, null);
  }

  public JobStatusQueryResponse queryJobStatus(
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      String targetedRegions) {
    return queryJobStatus(kafkaTopic, incrementalPushVersion, QUERY_JOB_STATUS_TIMEOUT, targetedRegions);
  }

  public JobStatusQueryResponse queryJobStatus(
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      int timeoutMs,
      String targetedRegions) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    QueryParams params =
        newParams().add(NAME, storeName).add(VERSION, version).add(INCREMENTAL_PUSH_VERSION, incrementalPushVersion);

    if (StringUtils.isNotEmpty(targetedRegions)) {
      params.add(TARGETED_REGIONS, targetedRegions);
    }
    return request(ControllerRoute.JOB, params, JobStatusQueryResponse.class, timeoutMs, 1, null);
  }

  /**
   * This method will retrieve detailed job status, including uncompleted partitions and replicas from a child region.
   * If the request is sent to a parent controller, it will be forwarded to a child controller in the specified region.
   * This method is used for data recovery progress monitoring.
   */
  public JobStatusQueryResponse queryDetailedJobStatus(String kafkaTopic, String region) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    QueryParams params = newParams().add(NAME, storeName).add(VERSION, version).add(FABRIC, region);
    return request(ControllerRoute.JOB, params, JobStatusQueryResponse.class, QUERY_JOB_STATUS_TIMEOUT, 1, null);
  }

  public ControllerResponse sendPushJobDetails(String storeName, int version, byte[] pushJobDetails) {
    QueryParams params = newParams().add(NAME, storeName).add(VERSION, version);
    return request(ControllerRoute.SEND_PUSH_JOB_DETAILS, params, ControllerResponse.class, pushJobDetails);
  }

  public MultiStoreResponse queryStoreList() {
    return queryStoreList(true);
  }

  public MultiStoreResponse queryStoreList(boolean includeSystemStores) {
    return queryStoreList(includeSystemStores, Optional.empty(), Optional.empty());
  }

  public MultiStoreResponse queryStoreList(
      boolean includeSystemStores,
      Optional<String> configNameFilter,
      Optional<String> configValueFilter) {
    QueryParams queryParams = newParams().add(INCLUDE_SYSTEM_STORES, includeSystemStores);
    if (configNameFilter.isPresent() ^ configValueFilter.isPresent()) {
      throw new VeniceException(
          "Missing argument: "
              + (configNameFilter.isPresent() ? "store-config-value-filter" : "store-config-name-filter"));
    }
    if (includeSystemStores && configNameFilter.isPresent()) {
      throw new VeniceException("Doesn't support config filtering on system store yet.");
    }
    configNameFilter.ifPresent(c -> queryParams.add(STORE_CONFIG_NAME_FILTER, c));
    configValueFilter.ifPresent(c -> queryParams.add(STORE_CONFIG_VALUE_FILTER, c));
    return request(ControllerRoute.LIST_STORES, queryParams, MultiStoreResponse.class);
  }

  public MultiStoreStatusResponse listStoresStatuses() {
    return request(ControllerRoute.CLUSTER_HEALTH_STORES, newParams(), MultiStoreStatusResponse.class);
  }

  public ControllerResponse enableStoreWrites(String storeName, boolean enable) {
    return enableStore(storeName, enable, WRITE_OPERATION);
  }

  public ControllerResponse enableStoreReads(String storeName, boolean enable) {
    return enableStore(storeName, enable, READ_OPERATION);
  }

  public ControllerResponse enableStoreReadWrites(String storeName, boolean enable) {
    return enableStore(storeName, enable, READ_WRITE_OPERATION);
  }

  private ControllerResponse enableStore(String storeName, boolean enable, String operation) {
    QueryParams params = newParams().add(NAME, storeName).add(STATUS, enable).add(OPERATION, operation);
    return request(ControllerRoute.ENABLE_STORE, params, ControllerResponse.class);
  }

  public MultiVersionResponse deleteAllVersions(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.DELETE_ALL_VERSIONS, params, MultiVersionResponse.class);
  }

  public VersionResponse deleteOldVersion(String storeName, int versionNum) {
    QueryParams params = newParams().add(NAME, storeName).add(VERSION, versionNum);
    return request(ControllerRoute.DELETE_OLD_VERSION, params, VersionResponse.class);
  }

  public NodeStatusResponse isNodeRemovable(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.NODE_REMOVABLE, params, NodeStatusResponse.class);
  }

  public NodeStatusResponse isNodeRemovable(String instanceId, List<String> lockedNodeIds) {
    QueryParams params =
        newParams().add(STORAGE_NODE_ID, instanceId).add(TO_BE_STOPPED_INSTANCES, String.join(",", lockedNodeIds));
    return request(ControllerRoute.NODE_REMOVABLE, params, NodeStatusResponse.class);
  }

  public ControllerResponse addNodeIntoAllowList(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.ALLOW_LIST_ADD_NODE, params, ControllerResponse.class);
  }

  public ControllerResponse removeNodeFromAllowList(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.ALLOW_LIST_REMOVE_NODE, params, ControllerResponse.class);
  }

  public ControllerResponse removeNodeFromCluster(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.REMOVE_NODE, params, ControllerResponse.class);
  }

  public MultiNodeResponse listStorageNodes() {
    return request(ControllerRoute.LIST_NODES, newParams(), MultiNodeResponse.class);
  }

  public StoppableNodeStatusResponse getAggregatedHealthStatus(
      String clusterName,
      List<String> instances,
      List<String> toBeStoppedInstances) throws JsonProcessingException {

    AggregatedHealthStatusRequest request =
        new AggregatedHealthStatusRequest(clusterName, instances, toBeStoppedInstances);
    String requestString = OBJECT_MAPPER.writeValueAsString(request);
    return request(
        ControllerRoute.AGGREGATED_HEALTH_STATUS,
        newParams(),
        StoppableNodeStatusResponse.class,
        requestString.getBytes());
  }

  public MultiNodesStatusResponse listInstancesStatuses(boolean enableReplicas) {
    QueryParams params = newParams();
    if (enableReplicas) {
      params.add(ENABLE_DISABLED_REPLICAS, "true");
    }
    return request(ControllerRoute.ClUSTER_HEALTH_INSTANCES, params, MultiNodesStatusResponse.class);
  }

  public MultiReplicaResponse listReplicas(String storeName, int version) {
    QueryParams params = newParams().add(NAME, storeName).add(VERSION, version);
    return request(ControllerRoute.LIST_REPLICAS, params, MultiReplicaResponse.class);
  }

  public MultiReplicaResponse listStorageNodeReplicas(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.NODE_REPLICAS, params, MultiReplicaResponse.class);
  }

  public NodeReplicasReadinessResponse nodeReplicasReadiness(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.NODE_REPLICAS_READINESS, params, NodeReplicasReadinessResponse.class);
  }

  public ChildAwareResponse listChildControllers(String clusterName) {
    QueryParams params = newParams().add(CLUSTER, clusterName);
    return request(ControllerRoute.LIST_CHILD_CLUSTERS, params, ChildAwareResponse.class);
  }

  public SchemaResponse getKeySchema(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.GET_KEY_SCHEMA, params, SchemaResponse.class);
  }

  public SchemaResponse addValueSchema(String storeName, String valueSchemaStr) {
    QueryParams params = newParams().add(NAME, storeName).add(VALUE_SCHEMA, valueSchemaStr);
    return request(ControllerRoute.ADD_VALUE_SCHEMA, params, SchemaResponse.class);
  }

  public SchemaResponse addValueSchema(String storeName, String valueSchemaStr, int valueSchemaId) {
    QueryParams params =
        newParams().add(NAME, storeName).add(VALUE_SCHEMA, valueSchemaStr).add(SCHEMA_ID, valueSchemaId);
    return request(ControllerRoute.ADD_VALUE_SCHEMA, params, SchemaResponse.class);
  }

  public SchemaResponse addValueSchema(
      String storeName,
      String valueSchemaStr,
      int valueSchemaId,
      DirectionalSchemaCompatibilityType schemaCompatType) {
    QueryParams params = newParams().add(NAME, storeName)
        .add(VALUE_SCHEMA, valueSchemaStr)
        .add(SCHEMA_ID, valueSchemaId)
        .add(SCHEMA_COMPAT_TYPE, schemaCompatType.toString());
    return request(ControllerRoute.ADD_VALUE_SCHEMA, params, SchemaResponse.class);
  }

  public SchemaResponse addDerivedSchema(String storeName, int valueSchemaId, String derivedSchemaStr) {
    QueryParams params =
        newParams().add(NAME, storeName).add(SCHEMA_ID, valueSchemaId).add(DERIVED_SCHEMA, derivedSchemaStr);
    return request(ControllerRoute.ADD_DERIVED_SCHEMA, params, SchemaResponse.class);
  }

  public SchemaResponse addDerivedSchema(
      String storeName,
      int valueSchemaId,
      String derivedSchemaStr,
      int derivedSchemaId) {
    QueryParams params = newParams().add(NAME, storeName)
        .add(SCHEMA_ID, valueSchemaId)
        .add(DERIVED_SCHEMA, derivedSchemaStr)
        .add(DERIVED_SCHEMA_ID, derivedSchemaId);
    return request(ControllerRoute.ADD_DERIVED_SCHEMA, params, SchemaResponse.class);
  }

  public SchemaResponse removeDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId) {
    QueryParams params =
        newParams().add(NAME, storeName).add(SCHEMA_ID, valueSchemaId).add(DERIVED_SCHEMA_ID, derivedSchemaId);
    return request(ControllerRoute.REMOVE_DERIVED_SCHEMA, params, SchemaResponse.class);
  }

  public PartitionResponse setStorePartitionCount(String storeName, String partitionNum) {
    QueryParams params = newParams().add(NAME, storeName).add(PARTITION_COUNT, partitionNum);
    return request(ControllerRoute.SET_PARTITION_COUNT, params, PartitionResponse.class);
  }

  public OwnerResponse setStoreOwner(String storeName, String owner) {
    QueryParams params = newParams().add(NAME, storeName).add(OWNER, owner);
    return request(ControllerRoute.SET_OWNER, params, OwnerResponse.class);
  }

  public ControllerResponse updateStore(String storeName, UpdateStoreQueryParams queryParams) {
    QueryParams params = addCommonParams(queryParams).add(NAME, storeName);
    return request(ControllerRoute.UPDATE_STORE, params, ControllerResponse.class);
  }

  public SchemaResponse getValueSchema(String storeName, int valueSchemaId) {
    QueryParams params = newParams().add(NAME, storeName).add(SCHEMA_ID, valueSchemaId);
    return request(ControllerRoute.GET_VALUE_SCHEMA, params, SchemaResponse.class);
  }

  public SchemaResponse getValueSchemaID(String storeName, String valueSchemaStr) {
    QueryParams params = newParams().add(NAME, storeName).add(VALUE_SCHEMA, valueSchemaStr);
    return request(ControllerRoute.GET_VALUE_SCHEMA_ID, params, SchemaResponse.class);
  }

  public SchemaResponse getValueOrDerivedSchemaId(String storeName, String derivedSchemaStr) {
    QueryParams params = newParams().add(NAME, storeName).add(DERIVED_SCHEMA, derivedSchemaStr);
    return request(ControllerRoute.GET_VALUE_OR_DERIVED_SCHEMA_ID, params, SchemaResponse.class);
  }

  public MultiSchemaResponse getAllValueSchema(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.GET_ALL_VALUE_SCHEMA, params, MultiSchemaResponse.class);
  }

  public MultiSchemaResponse getAllReplicationMetadataSchemas(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.GET_ALL_REPLICATION_METADATA_SCHEMAS, params, MultiSchemaResponse.class);
  }

  public MultiSchemaResponse getAllValueAndDerivedSchema(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.GET_ALL_VALUE_AND_DERIVED_SCHEMA, params, MultiSchemaResponse.class);
  }

  public AdminCommandExecutionResponse getAdminCommandExecution(long executionId) {
    QueryParams params = newParams().add(EXECUTION_ID, executionId);
    return request(ControllerRoute.EXECUTION, params, AdminCommandExecutionResponse.class);
  }

  public LastSucceedExecutionIdResponse getLastSucceedExecutionId() {
    return request(ControllerRoute.LAST_SUCCEED_EXECUTION_ID, newParams(), LastSucceedExecutionIdResponse.class);
  }

  public ControllerResponse enableThrottling(boolean isThrottlingEnabled) {
    QueryParams params = newParams().add(STATUS, isThrottlingEnabled);
    return request(ControllerRoute.ENABLE_THROTTLING, params, ControllerResponse.class);
  }

  public ControllerResponse enableMaxCapacityProtection(boolean isMaxCapacityProtection) {
    QueryParams params = newParams().add(STATUS, isMaxCapacityProtection);
    return request(ControllerRoute.ENABLE_MAX_CAPACITY_PROTECTION, params, ControllerResponse.class);
  }

  public ControllerResponse enableQuotaRebalanced(boolean isQuotaRebalanced, int expectRouterCount) {
    QueryParams params = newParams().add(STATUS, isQuotaRebalanced).add(EXPECTED_ROUTER_COUNT, expectRouterCount);
    return request(ControllerRoute.ENABLE_QUOTA_REBALANCED, params, ControllerResponse.class);
  }

  public RoutersClusterConfigResponse getRoutersClusterConfig() {
    return request(ControllerRoute.GET_ROUTERS_CLUSTER_CONFIG, newParams(), RoutersClusterConfigResponse.class);
  }

  public MigrationPushStrategyResponse getMigrationPushStrategies() {
    return request(ControllerRoute.GET_ALL_MIGRATION_PUSH_STRATEGIES, newParams(), MigrationPushStrategyResponse.class);
  }

  public ControllerResponse setMigrationPushStrategy(String voldemortStoreName, String pushStrategy) {
    QueryParams params = newParams().add(VOLDEMORT_STORE_NAME, voldemortStoreName).add(PUSH_STRATEGY, pushStrategy);
    return request(ControllerRoute.SET_MIGRATION_PUSH_STRATEGY, params, MigrationPushStrategyResponse.class);
  }

  public MultiVersionStatusResponse listBootstrappingVersions() {
    return request(ControllerRoute.LIST_BOOTSTRAPPING_VERSIONS, newParams(), MultiVersionStatusResponse.class);
  }

  public AclResponse updateAclForStore(String storeName, String accessPermissions) {
    QueryParams params = newParams().add(NAME, storeName).add(ACCESS_PERMISSION, accessPermissions);
    return request(ControllerRoute.UPDATE_ACL, params, AclResponse.class);
  }

  public AclResponse getAclForStore(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.GET_ACL, params, AclResponse.class);
  }

  public AclResponse deleteAclForStore(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.DELETE_ACL, params, AclResponse.class);
  }

  public ControllerResponse sendHeartbeatToSystemStore(String storeName, long heartbeatTimestamp) {
    QueryParams params = newParams().add(NAME, storeName).add(HEARTBEAT_TIMESTAMP, heartbeatTimestamp);
    return request(ControllerRoute.SEND_HEARTBEAT_TIMESTAMP_TO_SYSTEM_STORE, params, ControllerResponse.class);
  }

  public SystemStoreHeartbeatResponse getHeartbeatFromSystemStore(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(
        ControllerRoute.GET_HEARTBEAT_TIMESTAMP_FROM_SYSTEM_STORE,
        params,
        SystemStoreHeartbeatResponse.class);
  }

  public ControllerResponse configureActiveActiveReplicationForCluster(
      boolean enableActiveActiveReplication,
      String storeType,
      Optional<String> regionsFilter) {
    // Verify the input storeType is valid
    VeniceUserStoreType.valueOf(storeType.toUpperCase());
    QueryParams params = newParams().add(STATUS, enableActiveActiveReplication).add(STORE_TYPE, storeType);
    regionsFilter.ifPresent(f -> params.add(REGIONS_FILTER, f));
    return request(ControllerRoute.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER, params, ControllerResponse.class);
  }

  public ControllerResponse checkResourceCleanupForStoreCreation(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.CHECK_RESOURCE_CLEANUP_FOR_STORE_CREATION, params, ControllerResponse.class);
  }

  protected static QueryParams getQueryParamsToDiscoverCluster(String storeName) {
    return new QueryParams()
        // Cluster name is not required for cluster discovery request. But could not null otherwise an exception will be
        // thrown on server side.
        .add(CLUSTER, "*")
        .add(NAME, storeName);
  }

  public MultiStoreTopicsResponse getDeletableStoreTopics() {
    return request(ControllerRoute.GET_DELETABLE_STORE_TOPICS, newParams(), MultiStoreTopicsResponse.class);
  }

  public ClusterStaleDataAuditResponse getClusterStaleStores(String clusterName, String parentControllerUrl) {
    QueryParams params = newParams().add(CLUSTER, clusterName);
    try (ControllerTransport transport = new ControllerTransport(sslFactory)) {
      return transport.request(
          parentControllerUrl,
          ControllerRoute.GET_STALE_STORES_IN_CLUSTER,
          params,
          ClusterStaleDataAuditResponse.class);
    } catch (Exception e) {
      return makeErrorResponse(
          "Failed to get stale stores in cluster: " + clusterName,
          e,
          ClusterStaleDataAuditResponse.class);
    }
  }

  public MultiStoreInfoResponse getClusterStores(String clusterName) {
    QueryParams params = newParams().add(CLUSTER, clusterName);
    return request(ControllerRoute.GET_STORES_IN_CLUSTER, params, MultiStoreInfoResponse.class);
  }

  public VersionResponse getStoreLargestUsedVersion(String clusterName, String storeName) {
    QueryParams params = newParams().add(CLUSTER, clusterName).add(NAME, storeName);
    return request(ControllerRoute.GET_STORE_LARGEST_USED_VERSION, params, VersionResponse.class);
  }

  public RegionPushDetailsResponse getRegionPushDetails(String storeName, boolean isPartitionDetailEnabled) {
    QueryParams params = newParams().add(NAME, storeName).add(PARTITION_DETAIL_ENABLED, isPartitionDetailEnabled);
    return request(ControllerRoute.GET_REGION_PUSH_DETAILS, params, RegionPushDetailsResponse.class);
  }

  public StoreHealthAuditResponse listStorePushInfo(String storeName, boolean isPartitionDetailEnabled) {
    QueryParams params = newParams().add(NAME, storeName).add(PARTITION_DETAIL_ENABLED, isPartitionDetailEnabled);
    return request(ControllerRoute.LIST_STORE_PUSH_INFO, params, StoreHealthAuditResponse.class);
  }

  public static D2ServiceDiscoveryResponse discoverCluster(
      String discoveryUrls,
      String storeName,
      Optional<SSLFactory> sslFactory,
      int retryAttempts) {
    try (ControllerClient client = ControllerClientFactory.getControllerClient("*", discoveryUrls, sslFactory)) {
      return retryableRequest(client, retryAttempts, c -> c.discoverCluster(storeName));
    }
  }

  public D2ServiceDiscoveryResponse discoverCluster(String storeName) {
    List<String> urls = new ArrayList<>(this.controllerDiscoveryUrls);
    Collections.shuffle(urls);

    Exception lastConnectException = null;
    Exception lastException = null;
    try (ControllerTransport transport = new ControllerTransport(sslFactory)) {
      for (String url: urls) {
        try {
          // Because the way to get parameter is different between controller and router, in order to support query
          // cluster from both cluster and router, we send the path "/discover_cluster?storename=$storeName" at first,
          // if it does not work, try "/discover_cluster/$storeName"
          try {
            QueryParams params = getQueryParamsToDiscoverCluster(storeName);
            return transport.request(url, ControllerRoute.CLUSTER_DISCOVERY, params, D2ServiceDiscoveryResponse.class);
          } catch (VeniceHttpException e) {
            // TODO: Routers also support fetching the store name via query params. So, once sufficient time has passed,
            // this check can be changed to break out of the loop on non-5XX errors.

            // Do not attempt querying further if host explicitly returns that store was not found.
            // If Controllers have been upgraded to recent versions, they will return the proper STORE_NOT_FOUND
            // ErrorType.
            if (e.getErrorType() == ErrorType.STORE_NOT_FOUND) {
              lastException = e;
              break;
            }

            // If Controllers have not been upgraded recently, they will return a 404 status with GENERAL_ERROR as the
            // ErrorType.
            if (e.getErrorType() == ErrorType.GENERAL_ERROR && e.getHttpStatusCode() == 404) {
              lastException =
                  new VeniceHttpException(e.getHttpStatusCode(), e.getMessage(), e, ErrorType.STORE_NOT_FOUND);
              break;
            }

            String routerPath = ControllerRoute.CLUSTER_DISCOVERY.getPath() + "/" + storeName;
            return transport.executeGet(url, routerPath, new QueryParams(), D2ServiceDiscoveryResponse.class);
          }
        } catch (Exception e) {
          LOGGER.warn("Unable to discover cluster for store {} from {}", storeName, url);
          if (ExceptionUtils.recursiveClassEquals(e, ConnectException.class)) {
            lastConnectException = e;
          } else {
            lastException = e;
          }
        }
      }
    }

    /**
     * During normal operation, some hosts might be down for maintenance. Over time, the host list might even get
     * stale. So, when requests are made to hosts which no longer host venice controllers or routers,
     * {@link ConnectException} will be thrown. When the request actually leads to an error, all active hosts in
     * the host list will throw this error and the inactive hosts will throw the {@link ConnectException}. In such
     * cases, the {@link ConnectException} is not actionable by the user. If after trying all controllers and routers,
     * we still don't have any other non-{@link ConnectException}, the {@link ConnectException} is the most actionable
     * exception, and we throw that.
     */
    if (lastException == null) {
      lastException = lastConnectException;
    }

    String message = "Unable to discover cluster for store " + storeName + " from " + this.controllerDiscoveryUrls;
    return makeErrorResponse(message, lastException, D2ServiceDiscoveryResponse.class);
  }

  public ControllerResponse updateClusterConfig(UpdateClusterConfigQueryParams queryParams) {
    if (queryParams.getNameValuePairs().size() == 0) {
      throw new VeniceException("UpdateClusterConfig command didn't change any specific cluster config");
    }
    QueryParams params = addCommonParams(queryParams);
    return request(ControllerRoute.UPDATE_CLUSTER_CONFIG, params, ControllerResponse.class);
  }

  public ControllerResponse prepareDataRecovery(
      String sourceFabric,
      String destinationFabric,
      String storeName,
      int versionNumber,
      Optional<Integer> sourceAmplificationFactor) {
    QueryParams params = newParams().add(NAME, storeName)
        .add(SOURCE_FABRIC, sourceFabric)
        .add(FABRIC, destinationFabric)
        .add(VERSION, versionNumber);
    sourceAmplificationFactor.ifPresent(integer -> params.add(AMPLIFICATION_FACTOR, integer));
    return request(ControllerRoute.PREPARE_DATA_RECOVERY, params, ControllerResponse.class);
  }

  public ReadyForDataRecoveryResponse isStoreVersionReadyForDataRecovery(
      String sourceFabric,
      String destinationFabric,
      String storeName,
      int versionNumber,
      Optional<Integer> sourceAmplificationFactor) {
    QueryParams params = newParams().add(NAME, storeName)
        .add(SOURCE_FABRIC, sourceFabric)
        .add(FABRIC, destinationFabric)
        .add(VERSION, versionNumber);
    sourceAmplificationFactor.ifPresent(integer -> params.add(AMPLIFICATION_FACTOR, integer));
    return request(
        ControllerRoute.IS_STORE_VERSION_READY_FOR_DATA_RECOVERY,
        params,
        ReadyForDataRecoveryResponse.class);
  }

  public ControllerResponse dataRecovery(
      String sourceFabric,
      String destinationFabric,
      String storeName,
      int versionNumber,
      boolean sourceVersionIncluded,
      boolean copyAllVersionConfigs,
      Optional<Version> sourceVersion) {
    if (sourceVersionIncluded && !sourceVersion.isPresent()) {
      throw new VeniceException("Missing source Version but sourceVersionConfigIsIncluded is set to true");
    }
    QueryParams params = newParams();
    params.add(NAME, storeName)
        .add(SOURCE_FABRIC, sourceFabric)
        .add(FABRIC, destinationFabric)
        .add(VERSION, versionNumber)
        .add(SOURCE_FABRIC_VERSION_INCLUDED, sourceVersionIncluded)
        .add(DATA_RECOVERY_COPY_ALL_VERSION_CONFIGS, copyAllVersionConfigs);
    if (sourceVersionIncluded) {
      try {
        byte[] sourceVersionByteArray = versionVeniceJsonSerializer.serialize(sourceVersion.get(), "");
        return request(ControllerRoute.DATA_RECOVERY, params, ControllerResponse.class, sourceVersionByteArray);
      } catch (IOException e) {
        throw new VeniceException("Failed to serialize source Version object", e);
      }
    } else {
      return request(ControllerRoute.DATA_RECOVERY, params, ControllerResponse.class);
    }
  }

  public AdminTopicMetadataResponse getAdminTopicMetadata(Optional<String> storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.GET_ADMIN_TOPIC_METADATA, params, AdminTopicMetadataResponse.class);
  }

  public ControllerResponse updateAdminTopicMetadata(
      long executionId,
      Optional<String> storeName,
      Optional<Long> offset,
      Optional<Long> upstreamOffset) {
    QueryParams params = newParams().add(EXECUTION_ID, executionId)
        .add(NAME, storeName)
        .add(OFFSET, offset)
        .add(UPSTREAM_OFFSET, upstreamOffset);
    return request(ControllerRoute.UPDATE_ADMIN_TOPIC_METADATA, params, ControllerResponse.class);
  }

  public ControllerResponse deleteKafkaTopic(String topicName) {
    QueryParams params = newParams().add(TOPIC, topicName);
    return request(ControllerRoute.DELETE_KAFKA_TOPIC, params, ControllerResponse.class);
  }

  public ControllerResponse createStoragePersona(
      String name,
      long quota,
      Set<String> storesToEnforce,
      Set<String> owners) {
    QueryParams params = newParams().add(PERSONA_NAME, name)
        .add(PERSONA_QUOTA, quota)
        .putStringSet(PERSONA_STORES, storesToEnforce)
        .putStringSet(PERSONA_OWNERS, owners);
    return request(ControllerRoute.CREATE_STORAGE_PERSONA, params, ControllerResponse.class);
  }

  public StoragePersonaResponse getStoragePersona(String name) {
    QueryParams params = newParams().add(PERSONA_NAME, name);
    return request(ControllerRoute.GET_STORAGE_PERSONA, params, StoragePersonaResponse.class);
  }

  public ControllerResponse deleteStoragePersona(String name) {
    QueryParams params = newParams().add(PERSONA_NAME, name);
    return request(ControllerRoute.DELETE_STORAGE_PERSONA, params, ControllerResponse.class);
  }

  public ControllerResponse updateStoragePersona(String name, UpdateStoragePersonaQueryParams queryParams) {
    QueryParams params = addCommonParams(queryParams).add(PERSONA_NAME, name);
    return request(ControllerRoute.UPDATE_STORAGE_PERSONA, params, ControllerResponse.class);
  }

  public StoragePersonaResponse getStoragePersonaAssociatedWithStore(String name) {
    QueryParams params = newParams().add(NAME, name);
    return request(ControllerRoute.GET_STORAGE_PERSONA_ASSOCIATED_WITH_STORE, params, StoragePersonaResponse.class);
  }

  public MultiStoragePersonaResponse getClusterStoragePersonas() {
    QueryParams params = newParams();
    return request(ControllerRoute.GET_CLUSTER_STORAGE_PERSONAS, params, MultiStoragePersonaResponse.class);
  }

  public MultiStoreTopicsResponse cleanupInstanceCustomizedStates() {
    QueryParams params = newParams().add(CLUSTER, clusterName);
    return request(ControllerRoute.CLEANUP_INSTANCE_CUSTOMIZED_STATES, params, MultiStoreTopicsResponse.class);
  }

  public ControllerResponse removeStoreFromGraveyard(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.REMOVE_STORE_FROM_GRAVEYARD, params, ControllerResponse.class);
  }

  /***
   * Add all global parameters in this method. Always use a form of this method to generate
   * a new list of NameValuePair objects for making HTTP requests.
   * @return
   */
  protected QueryParams newParams() {
    return addCommonParams(new QueryParams());
  }

  private QueryParams addCommonParams(QueryParams params) {
    return params.add(CLUSTER, this.clusterName);
  }

  protected static String encodeQueryParams(QueryParams params) {
    return URLEncodedUtils.format(params.getNameValuePairs(), StandardCharsets.UTF_8);
  }

  private <T extends ControllerResponse> T request(ControllerRoute route, QueryParams params, Class<T> responseType) {
    return request(route, params, responseType, DEFAULT_REQUEST_TIMEOUT_MS, DEFAULT_MAX_ATTEMPTS, null);
  }

  private <T extends ControllerResponse> T request(
      ControllerRoute route,
      QueryParams params,
      Class<T> responseType,
      byte[] data) {
    return request(route, params, responseType, DEFAULT_REQUEST_TIMEOUT_MS, DEFAULT_MAX_ATTEMPTS, data);
  }

  private <T extends ControllerResponse> T request(
      ControllerRoute route,
      QueryParams params,
      Class<T> responseType,
      int timeoutMs,
      int maxAttempts,
      byte[] data) {
    Exception lastException = null;
    boolean logErrorMessage = true;
    try (ControllerTransport transport = new ControllerTransport(sslFactory)) {
      for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
        try {
          return transport.request(getLeaderControllerUrl(), route, params, responseType, timeoutMs, data);
        } catch (ExecutionException | TimeoutException e) {
          // Controller is unreachable. Let's wait for a new leader to be elected.
          // Total wait time should be at least leader election time (~30 seconds)
          lastException = e;
        } catch (VeniceHttpException e) {
          if (e.getHttpStatusCode() == HttpStatus.SC_PRECONDITION_FAILED) {
            logErrorMessage = false;
            throw e;
          }
          if (e.getHttpStatusCode() != HttpConstants.SC_MISDIRECTED_REQUEST) {
            throw e;
          }
          // leader controller has changed. Let's wait for a new leader to realize it.
          lastException = e;
        } catch (Exception e) {
          lastException = e;
        }

        if (attempt < maxAttempts) {
          LOGGER.info(
              "Retrying controller request, attempt = {}/{}, controller = {}, route = {}, params = {}, timeout = {}",
              attempt,
              maxAttempts,
              this.leaderControllerUrl,
              route.getPath(),
              params.getNameValuePairs(),
              timeoutMs,
              lastException);
          Utils.sleep(5 * Time.MS_PER_SECOND);
        }
      }
    } catch (Exception e) {
      lastException = e;
    }

    String message =
        "An error occurred during controller request." + " controller = " + this.leaderControllerUrl + ", route = "
            + route.getPath() + ", params = " + params.getAbbreviatedNameValuePairs() + ", timeout = " + timeoutMs;
    return makeErrorResponse(message, lastException, responseType, logErrorMessage);
  }

  private static <T extends ControllerResponse> T makeErrorResponse(
      String message,
      Exception exception,
      Class<T> responseType) {
    return makeErrorResponse(message, exception, responseType, true);
  }

  private static <T extends ControllerResponse> T makeErrorResponse(
      String message,
      Exception exception,
      Class<T> responseType,
      boolean logErrorMessage) {
    if (logErrorMessage) {
      LOGGER.error(message, exception);
    }
    try {
      T response = responseType.newInstance();
      response.setError(message, exception);
      return response;
    } catch (InstantiationException | IllegalAccessException e) {
      LOGGER.error("Unable to instantiate controller response {}", responseType.getName(), e);
      throw new VeniceException(message, exception);
    }
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public String getLeaderControllerUrl() {
    this.leaderControllerUrl = discoverLeaderController();
    return this.leaderControllerUrl;
  }

  public Collection<String> getControllerDiscoveryUrls() {
    return this.controllerDiscoveryUrls;
  }
}
