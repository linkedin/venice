package com.linkedin.venice.controllerapi;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.controllerapi.routes.PushJobStatusUploadResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.status.protocol.enums.PushJobStatus;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;

import java.util.List;
import java.util.Optional;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;
import java.util.Properties;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.function.Function;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;
import org.apache.http.client.utils.URLEncodedUtils;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

public class ControllerClient implements Closeable {
  private static final int DEFAULT_MAX_ATTEMPTS = 10;
  private static final int QUERY_JOB_STATUS_TIMEOUT = 60 * Time.MS_PER_SECOND;
  private String clusterName;
  private String localHostName;
  private String masterControllerUrl;
  private List<String> controllerDiscoveryUrls;

  private final static Logger logger = Logger.getLogger(ControllerClient.class);

  /**
   * @param discoveryUrls comma-delimited urls to find master controller.
   */
  public ControllerClient(String clusterName, String discoveryUrls) {
    if (Utils.isNullOrEmpty(discoveryUrls)) {
      throw new VeniceException("Controller discovery url list is empty: " + discoveryUrls);
    }

    this.clusterName = clusterName;
    this.localHostName = Utils.getHostName();
    this.controllerDiscoveryUrls = Arrays.stream(discoveryUrls.split(",")).map(String::trim).collect(Collectors.toList());
    if (this.controllerDiscoveryUrls.isEmpty()) {
      throw new VeniceException("Controller discovery url list is empty");
    }
    logger.debug("Parsed hostname as: " + this.localHostName);
  }

  @Override
  public void close() {
  }

  protected String discoverMasterController() {
    List<String> urls = new ArrayList<>(this.controllerDiscoveryUrls);
    Collections.shuffle(urls);

    Exception lastException = null;
    try (ControllerTransport transport = new ControllerTransport()) {
      for (String url : urls) {
        try {
          String masterUrl = transport.request(url, ControllerRoute.MASTER_CONTROLLER, newParams(), MasterControllerResponse.class).getUrl();
          logger.info("Discovered master controller " + masterUrl + " from " + url);
          return masterUrl;
        } catch (Exception e) {
          logger.warn("Unable to discover master controller from " + url, e);
          lastException = e;
        }
      }
    }
    String message = "Unable to discover master controller from " + this.controllerDiscoveryUrls;
    logger.error(message, lastException);
    throw new VeniceException(message, lastException);
  }

  public StoreResponse getStore(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.STORE, params, StoreResponse.class);
  }

  @Deprecated
  public static StoreResponse getStore(String urlsToFindMasterController, String clusterName, String storeName) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)) {
      return client.getStore(storeName);
    }
  }

  public StorageEngineOverheadRatioResponse getStorageEngineOverheadRatio(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.STORAGE_ENGINE_OVERHEAD_RATIO, params, StorageEngineOverheadRatioResponse.class);
  }

  /**
   * Request a topic for the VeniceWriter to write into.  A new H2V push, or a Samza bulk processing job should both use
   * this method.  The push job ID needs to be unique for this push.  Multiple requests with the same pushJobId are
   * idempotent and will return the same topic.
   * @param storeName Name of the store being written to.
   * @param storeSize Estimated size of push in bytes, used to determine partitioning
   * @param pushJobId Unique Id for this job
   * @param sendStartOfPush Whether controller should send START_OF_PUSH message to the newly created topic,
   *                        while adding a new version. This is currently used in Samza batch load, a.k.a. grandfather
   * @param sorted Whether the push is going to contain sorted data (in each partition) or not
   * @return VersionCreationResponse includes topic and partitioning
   */
  public VersionCreationResponse requestTopicForWrites(String storeName, long storeSize, PushType pushType,
      String pushJobId, boolean sendStartOfPush, boolean sorted) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(STORE_SIZE, Long.toString(storeSize))
        .add(PUSH_JOB_ID, pushJobId)
        .add(PUSH_TYPE, pushType.toString())
        .add(SEND_START_OF_PUSH, sendStartOfPush)
        .add(PUSH_IN_SORTED_ORDER, sorted);
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
   * @return
   */
  public VersionResponse addVersionAndStartIngestion(String storeName, String pushJobId, int version, int partitionCount) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(PUSH_JOB_ID, pushJobId)
        .add(VERSION, version)
        .add(PARTITION_COUNT, partitionCount);
    return request(ControllerRoute.ADD_VERSION, params, VersionResponse.class);
  }

  public ControllerResponse uploadPushProperties(String storeName, int version, Properties properties) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(VERSION, version);
    for (Object key : properties.keySet()) {
      params.add(key.toString(), properties.getProperty(key.toString()));
    }
    return request(ControllerRoute.OFFLINE_PUSH_INFO, params, ControllerResponse.class);
  }

  public ControllerResponse writeEndOfPush(String storeName, int version) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(VERSION, version);
    return request(ControllerRoute.END_OF_PUSH, params, ControllerResponse.class);
  }

  public VersionCreationResponse emptyPush(String storeName, String pushJobId, long storeSize) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(PUSH_JOB_ID, pushJobId)
        .add(STORE_SIZE, Long.toString(storeSize));
    return request(ControllerRoute.EMPTY_PUSH, params, VersionCreationResponse.class);
  }

  public NewStoreResponse createNewStore(String storeName, String owner, String keySchema, String valueSchema) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(OWNER, owner)
        .add(KEY_SCHEMA, keySchema)
        .add(VALUE_SCHEMA, valueSchema);
    return request(ControllerRoute.NEW_STORE, params, NewStoreResponse.class);
  }

  public StoreMigrationResponse migrateStore(String storeName, String srcClusterName) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(CLUSTER_SRC, srcClusterName);
    return request(ControllerRoute.MIGRATE_STORE, params, StoreMigrationResponse.class);
  }

  /**
   * This commmand should be sent to src controller, not dest controler
   */
  public StoreMigrationResponse abortMigration(String storeName, String destClusterName) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(CLUSTER_DEST, destClusterName);
    return request(ControllerRoute.ABORT_MIGRATION, params, StoreMigrationResponse.class);
  }

  public TrackableControllerResponse deleteStore(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.DELETE_STORE, params, TrackableControllerResponse.class);
  }

  public VersionResponse overrideSetActiveVersion(String storeName, int version) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(VERSION, version);
    return request(ControllerRoute.SET_VERSION, params, VersionResponse.class);
  }

  public ControllerResponse killOfflinePushJob(String kafkaTopic) {
    String store = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int versionNumber = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    QueryParams params = newParams()
        .add(TOPIC, kafkaTopic) // TODO: remove once the controller is deployed to handle store and version instead
        .add(NAME, store)
        .add(VERSION, versionNumber);
    return request(ControllerRoute.KILL_OFFLINE_PUSH_JOB, params, ControllerResponse.class);
  }

  public ControllerResponse skipAdminMessage(String offset, boolean skipDIV) {
    QueryParams params = newParams()
        .add(OFFSET, offset)
        .add(SKIP_DIV, skipDIV);
    return request(ControllerRoute.SKIP_ADMIN, params, ControllerResponse.class);
  }

  public <R extends ControllerResponse> R retryableRequest(int totalAttempts, Function<ControllerClient, R> request) {
    return retryableRequest(this, totalAttempts, request);
  }

  /**
   * Useful for pieces of code which want to have a test mocking the result of the function that's passed in...
   */
  public static <R extends ControllerResponse> R retryableRequest(ControllerClient client, int totalAttempts, Function<ControllerClient, R> request) {
    if (totalAttempts < 1) {
      throw new VeniceException("Querying with retries requires at least one attempt, called with " + totalAttempts + " attempts");
    }
    int currentAttempt = 1;
    while (true) {
      R response = request.apply(client);
      if (!response.isError() || currentAttempt == totalAttempts) {
        return response;
      } else {
        logger.warn("Error on attempt " + currentAttempt + "/" + totalAttempts + " of querying the Controller: " + response.getError());
        currentAttempt++;
        Utils.sleep(2000);
      }
    }
  }

  /**
   * This method has a longer timeout intended to be used to query the overall job status on a parent controller. The
   * extended timeout is meant for the parent controller to query each colo's child controller for the job status and
   * aggregate the results. Use {@link ControllerClient#queryJobStatus(String, Optional)} instead if the target is a
   * child controller.
   */
  public JobStatusQueryResponse queryOverallJobStatus(String kafkaTopic, Optional<String> incrementalPushVersion) {
    return queryJobStatus(kafkaTopic, incrementalPushVersion, 5 * QUERY_JOB_STATUS_TIMEOUT);
  }

  public JobStatusQueryResponse queryJobStatus(String kafkaTopic) {
    return queryJobStatus(kafkaTopic, Optional.empty(), QUERY_JOB_STATUS_TIMEOUT);
  }

  /**
   * This method is used to query the job status from a controller. It is expected to be a child controller thus a
   * shorter timeout is enforced. Use {@link ControllerClient#queryOverallJobStatus(String, Optional)} instead if the
   * target is a parent controller.
   */
  public JobStatusQueryResponse queryJobStatus(String kafkaTopic, Optional<String> incrementalPushVersion) {
    return queryJobStatus(kafkaTopic, incrementalPushVersion, QUERY_JOB_STATUS_TIMEOUT);
  }

  public JobStatusQueryResponse queryJobStatus(String kafkaTopic, Optional<String> incrementalPushVersion,
      int timeoutMs) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(VERSION, version)
        .add(INCREMENTAL_PUSH_VERSION, incrementalPushVersion);
    return request(ControllerRoute.JOB, params, JobStatusQueryResponse.class, timeoutMs, 1);
  }

  /**
   * Uploads information regarding to a push job
   * @param storeName name of the store involved with the push job
   * @param version version number of the involved store
   * @param status the final status of the push job
   * @param jobDurationInMs duration of the push job in milliseconds
   * @param pushJobId unique id of the push job
   * @param message additional description for the corresponding job status
   * @return
   */
  public PushJobStatusUploadResponse uploadPushJobStatus(String storeName, int version, PushJobStatus status,
      long jobDurationInMs, String pushJobId, String message) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(VERSION, version)
        .add(PUSH_JOB_STATUS, status.name())
        .add(PUSH_JOB_DURATION, jobDurationInMs)
        .add(PUSH_JOB_ID, pushJobId)
        .add(MESSAGE, message);
    return request(ControllerRoute.UPLOAD_PUSH_JOB_STATUS, params, PushJobStatusUploadResponse.class);
  }
  public MultiStoreResponse queryStoreList() {
    return queryStoreList(true);
  }

  public MultiStoreResponse queryStoreList(boolean includeSystemStores) {
    QueryParams queryParams = newParams()
        .add(INCLUDE_SYSTEM_STORES, includeSystemStores);
    return request(ControllerRoute.LIST_STORES, queryParams, MultiStoreResponse.class);
  }

  public MultiStoreStatusResponse listStoresStatuses() {
    return request(ControllerRoute.CLUSTER_HEALTH_STORES, newParams(),  MultiStoreStatusResponse.class);
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
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(STATUS, enable)
        .add(OPERATION, operation);
    return request(ControllerRoute.ENABLE_STORE, params, ControllerResponse.class);
  }

  public MultiVersionResponse deleteAllVersions(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.DELETE_ALL_VERSIONS, params, MultiVersionResponse.class);
  }

  public VersionResponse deleteOldVersion(String storeName, int versionNum) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(VERSION, versionNum);
    return request(ControllerRoute.DELETE_OLD_VERSION, params, VersionResponse.class);
  }

  public NodeStatusResponse isNodeRemovable(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.NODE_REMOVABLE, params, NodeStatusResponse.class);
  }

  public ControllerResponse addNodeIntoWhiteList(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.WHITE_LIST_ADD_NODE, params, ControllerResponse.class);
  }

  public ControllerResponse removeNodeFromWhiteList(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.WHITE_LIST_REMOVE_NODE, params, ControllerResponse.class);
  }

  public ControllerResponse removeNodeFromCluster(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.REMOVE_NODE, params, ControllerResponse.class);
  }

  public MultiNodeResponse listStorageNodes() {
    return request(ControllerRoute.LIST_NODES, newParams(), MultiNodeResponse.class);
  }

  public MultiNodesStatusResponse listInstancesStatuses() {
    return request(ControllerRoute.ClUSTER_HEALTH_INSTANCES, newParams(), MultiNodesStatusResponse.class);
  }

  public MultiReplicaResponse listReplicas(String storeName, int version) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(VERSION, version);
    return request(ControllerRoute.LIST_REPLICAS, params, MultiReplicaResponse.class);
  }

  public MultiReplicaResponse listStorageNodeReplicas(String instanceId) {
    QueryParams params = newParams().add(STORAGE_NODE_ID, instanceId);
    return request(ControllerRoute.NODE_REPLICAS, params, MultiReplicaResponse.class);
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
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(VALUE_SCHEMA, valueSchemaStr);
    return request(ControllerRoute.ADD_VALUE_SCHEMA, params, SchemaResponse.class);
  }

  public PartitionResponse setStorePartitionCount(String storeName, String partitionNum) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(PARTITION_COUNT, partitionNum);
    return request(ControllerRoute.SET_PARTITION_COUNT, params, PartitionResponse.class);
  }

  public OwnerResponse setStoreOwner(String storeName, String owner) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(OWNER, owner);
    return request(ControllerRoute.SET_OWNER, params, OwnerResponse.class);
  }

  public ControllerResponse updateStore(String storeName, UpdateStoreQueryParams queryParams) {
    QueryParams params = addCommonParams(queryParams).add(NAME, storeName);
    return request(ControllerRoute.UPDATE_STORE, params, ControllerResponse.class);
  }

  public SchemaResponse getValueSchema(String storeName, int valueSchemaId) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(SCHEMA_ID, valueSchemaId);
    return request(ControllerRoute.GET_VALUE_SCHEMA, params, SchemaResponse.class);
  }

  public SchemaResponse getValueSchemaID(String storeName, String valueSchemaStr) {
    QueryParams params = newParams()
        .add(NAME, storeName)
        .add(VALUE_SCHEMA, valueSchemaStr);
    return request(ControllerRoute.GET_VALUE_SCHEMA_ID, params, SchemaResponse.class);
  }

  public MultiSchemaResponse getAllValueSchema(String storeName) {
    QueryParams params = newParams().add(NAME, storeName);
    return request(ControllerRoute.GET_ALL_VALUE_SCHEMA, params, MultiSchemaResponse.class);
  }

  public AdminCommandExecutionResponse getAdminCommandExecution(long executionId) {
    QueryParams params = newParams().add(EXECUTION_ID, executionId);
    return request(ControllerRoute.EXECUTION, params, AdminCommandExecutionResponse.class);
  }

  public LastSucceedExecutionIdResponse getLastSucceedExecutionId() {
    return request(ControllerRoute.LAST_SUCCEED_EXECUTION_ID, newParams(), LastSucceedExecutionIdResponse.class);
  }

  public ControllerResponse enableThrotting(boolean isThrottlingEnabled) {
    QueryParams params = newParams().add(STATUS, isThrottlingEnabled);
    return request(ControllerRoute.ENABLE_THROTTLING, params, ControllerResponse.class);
  }

  public ControllerResponse enableMaxCapacityProtection(boolean isMaxCapacityProtion) {
    QueryParams params = newParams().add(STATUS, isMaxCapacityProtion);
    return request(ControllerRoute.ENABLE_MAX_CAPACITY_PROTECTION, params, ControllerResponse.class);
  }

  public ControllerResponse enableQuotaRebalanced(boolean isQuotaRebalanced, int expectRouterCount) {
    QueryParams params = newParams()
        .add(STATUS, isQuotaRebalanced)
        .add(EXPECTED_ROUTER_COUNT, expectRouterCount);
    return request(ControllerRoute.ENABLE_QUOTA_REBALANCED, params, ControllerResponse.class);
  }

  public RoutersClusterConfigResponse getRoutersClusterConfig() {
    return request(ControllerRoute.GET_ROUTERS_CLUSTER_CONFIG, newParams(), RoutersClusterConfigResponse.class);
  }

  public MigrationPushStrategyResponse getMigrationPushStrategies() {
    return request(ControllerRoute.GET_ALL_MIGRATION_PUSH_STRATEGIES, newParams(), MigrationPushStrategyResponse.class);
  }

  public ControllerResponse setMigrationPushStrategy(String voldemortStoreName, String pushStrategy) {
    QueryParams params = newParams()
        .add(VOLDEMORT_STORE_NAME, voldemortStoreName)
        .add(PUSH_STRATEGY, pushStrategy);
    return request(ControllerRoute.SET_MIGRATION_PUSH_STRATEGY, params, MigrationPushStrategyResponse.class);
  }

  public MultiVersionStatusResponse listBootstrappingVersions() {
    return request(ControllerRoute.LIST_BOOTSTRAPPING_VERSIONS, newParams(), MultiVersionStatusResponse.class);
  }

  protected static QueryParams getQueryParamsToDiscoverCluster(String storeName) {
    return new QueryParams()
        // Cluster name is not required for cluster discovery request. But could not null otherwise an exception will be
        // thrown on server side.
        .add(CLUSTER, "*")
        .add(HOSTNAME, Utils.getHostName())
        .add(NAME, storeName);
  }

  public static D2ServiceDiscoveryResponse discoverCluster(String discoveryUrls, String storeName) {
    try (ControllerClient client = new ControllerClient("*", discoveryUrls)) {
      return client.discoverCluster(storeName);
    }
  }

  public D2ServiceDiscoveryResponse discoverCluster(String storeName) {
    List<String> urls = new ArrayList<>(this.controllerDiscoveryUrls);
    Collections.shuffle(urls);

    Exception lastException = null;
    try (ControllerTransport transport = new ControllerTransport()) {
      for (String url : urls) {
        try {
          // Because the way to get parameter is different between controller and router, in order to support query cluster
          // from both cluster and router, we send the path "/discover_cluster?storename=$storeName" at first, if it does
          // not work, try "/discover_cluster/$storeName"
          try {
            QueryParams params = getQueryParamsToDiscoverCluster(storeName);
            return transport.request(url, ControllerRoute.CLUSTER_DISCOVERY, params, D2ServiceDiscoveryResponse.class);
          } catch (VeniceHttpException e) {
            String routerPath = ControllerRoute.CLUSTER_DISCOVERY.getPath() + "/" + storeName;
            return transport.executeGet(url, routerPath, new QueryParams(), D2ServiceDiscoveryResponse.class);
          }
        } catch (Exception e) {
          logger.warn("Unable to discover cluster for store " + storeName + " from " + url, e);
          lastException = e;
        }
      }
    }

    String message = "Unable to discover cluster for store " + storeName + " from " + this.controllerDiscoveryUrls;
    return makeErrorResponse(message, lastException, D2ServiceDiscoveryResponse.class);
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
    return params
        .add(HOSTNAME, this.localHostName)
        .add(CLUSTER, this.clusterName);
  }

  protected static String encodeQueryParams(QueryParams params) {
    return URLEncodedUtils.format(params.getNameValuePairs(), StandardCharsets.UTF_8);
  }

  private <T extends ControllerResponse> T request(ControllerRoute route, QueryParams params, Class<T> responseType) {
    return request(route, params, responseType, 600 * Time.MS_PER_SECOND, DEFAULT_MAX_ATTEMPTS);
  }

  private <T extends ControllerResponse> T request(ControllerRoute route, QueryParams params, Class<T> responseType,
      int timeoutMs, int maxAttempts) {
    Exception lastException = null;
    try (ControllerTransport transport = new ControllerTransport()) {
      for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
        try {
          return transport.request(getMasterControllerUrl(), route, params, responseType, timeoutMs);
        } catch (ExecutionException | TimeoutException e) {
          // Controller is unreachable. Let's wait for a new master to be elected.
          // Total wait time should be at least master election time (~30 seconds)
          lastException = e;
        } catch (VeniceHttpException e) {
          if (e.getHttpStatusCode() != HttpConstants.SC_MISDIRECTED_REQUEST) {
            throw e;
          }
          // Master controller has changed. Let's wait for a new master to realize it.
          lastException = e;
        }

        if (attempt < maxAttempts) {
          logger.info("Retrying controller request" +
                  ", attempt=" + attempt + "/" + maxAttempts +
                  ", controller=" + this.masterControllerUrl +
                  ", route=" + route.getPath() +
                  ", params=" + params.getNameValuePairs() +
                  ", timeout=" + timeoutMs,
              lastException);
          Utils.sleep(5 * Time.MS_PER_SECOND);
        }
      }
    } catch (Exception e) {
      lastException = e;
    }

    String message = "Unable to make controller request" +
        ", controller=" + this.masterControllerUrl +
        ", route=" + route.getPath() +
        ", params=" + params.getNameValuePairs() +
        ", timeout=" + timeoutMs;
    return makeErrorResponse(message, lastException, responseType);
  }

  private <T extends ControllerResponse> T makeErrorResponse(String message, Exception exception, Class<T> responseType) {
    logger.error(message, exception);
    try {
      T response = responseType.newInstance();
      response.setError(message + ", " +  exception.getMessage());
      return response;
    } catch (InstantiationException | IllegalAccessException e) {
      logger.error("Unable to instantiate controller response " + responseType.getName(), e);
      throw new VeniceException(message, exception);
    }
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public String getMasterControllerUrl() {
    this.masterControllerUrl = discoverMasterController();
    return this.masterControllerUrl;
  }

  public Collection<String> getControllerDiscoveryUrls() {
    return this.controllerDiscoveryUrls;
  }
}
