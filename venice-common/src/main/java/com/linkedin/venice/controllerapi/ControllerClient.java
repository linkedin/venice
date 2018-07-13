package com.linkedin.venice.controllerapi;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

public class ControllerClient implements Closeable {
  private String masterControllerUrl;
  private String clusterName;
  private String urlsToFindMasterController;
  private String localHostname;

  private static final int ADMIN_REQUEST_TIMEOUT_MIN = 10;
  protected final static ObjectMapper mapper = getObjectMapper();
  private final static Logger logger = Logger.getLogger(ControllerClient.class);

  protected static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    // Ignore unknown properties when deserializing json-encoded string
    objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    return objectMapper;
  }

  /**
   * @param urlsToFindMasterController comma-delimited urls to find master controller.
   */
  public ControllerClient(String clusterName, String urlsToFindMasterController) {
    if(Utils.isNullOrEmpty(urlsToFindMasterController)) {
      throw new VeniceException("urlsToFindMasterController: "+ urlsToFindMasterController +" is not valid");
    }

    this.clusterName = clusterName;
    this.urlsToFindMasterController = urlsToFindMasterController;
    this.localHostname = Utils.getHostName();
    if (logger.isDebugEnabled()) {
      logger.debug("Parsed hostname as: " + localHostname);
    }
  }

  protected void refreshControllerUrl(){
    String controllerUrl = getMasterControllerUrl(urlsToFindMasterController);
    if (controllerUrl.endsWith("/")){
      this.masterControllerUrl = controllerUrl.substring(0, controllerUrl.length()-1);
    } else {
      this.masterControllerUrl = controllerUrl;
    }
    logger.debug("Identified controller URL: " + this.masterControllerUrl + " from url: " + urlsToFindMasterController);
  }

  @Override
  public void close() {
  }

  protected String getMasterControllerUrl(String urlsToFindMasterController){
    List<String> urlList = Arrays.asList(urlsToFindMasterController.split(","));
    Collections.shuffle(urlList);
    Throwable lastException = null;
    for (String url : urlList) {
      try {
        String responseBody = getRequest(url, ControllerRoute.MASTER_CONTROLLER.getPath(), newParams());
        MasterControllerResponse controllerResponse = mapper.readValue(responseBody, MasterControllerResponse.class);
        if (controllerResponse.isError()) {
          throw new VeniceException("Received error response: [" + mapper.writeValueAsString(controllerResponse) + "] from url: " + url);
        }

        return controllerResponse.getUrl();
      } catch (Exception e) {
        logger.warn("Failed to get controller URL from url: " + url, e);
        lastException = e;
      }
    }
    String errMessage = "Could not get controller url from urls: " + urlsToFindMasterController;
    if (null != lastException) {
      throw new VeniceException(errMessage + " -- " + lastException.getMessage(), lastException);
    } else {
      throw new VeniceException(errMessage);
    }
  }

  public StoreResponse getStore(String storeName) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName);
      String responseJson = getRequest(ControllerRoute.STORE.getPath(), params);
      return mapper.readValue(responseJson, StoreResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting store: " + storeName, e), new StoreResponse());
    }
  }

  @Deprecated
  public static StoreResponse getStore(String urlsToFindMasterController, String clusterName, String storeName){
    try (ControllerClient client = new ControllerClient(clusterName,urlsToFindMasterController)){
      return client.getStore(storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting store: " + storeName, e), new StoreResponse());
    }
  }

  public StorageEngineOverheadRatioResponse getStorageEngineOverheadRatio(String storeName) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName);
      String responseJson = getRequest(ControllerRoute.STORAGE_ENGINE_OVERHEAD_RATIO.getPath(), params);
      return mapper.readValue(responseJson, StorageEngineOverheadRatioResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting store: " + storeName, e), new StorageEngineOverheadRatioResponse());
    }
  }

  /**
   * Use {@link #requestTopicForWrites(String, long, PushType, String)} instead
   * @param storeName
   * @param storeSize
   * @return
   */
  @Deprecated //Marked deprecated, but we should keep using this for H2V until we can do push ID based idempotent topic requests
  public VersionCreationResponse createNewStoreVersion(String storeName, long storeSize) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName)
          .add(STORE_SIZE, Long.toString(storeSize));
      String responseJson = postRequest(ControllerRoute.CREATE_VERSION.getPath(), params);
      return mapper.readValue(responseJson, VersionCreationResponse.class);
    } catch (Exception e){
      return handleError(
          new VeniceException("Error creating version for store: " + storeName, e), new VersionCreationResponse());
    }
  }

  /**
   * Request a topic for the VeniceWriter to write into.  A new H2V push, or a Samza bulk processing job should both use
   * this method.  The push job ID needs to be unique for this push.  Multiple requests with the same pushJobId are
   * idempotent and will return the same topic.
   * @param storeName Name of the store being written to.
   * @param storeSize Estimated size of push in bytes, used to determine partitioning
   * @param pushJobId Unique Id for this job
   * @return VersionCreationResponse includes topic and partitioning
   */
  public VersionCreationResponse requestTopicForWrites(String storeName, long storeSize, PushType pushType, String pushJobId) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName)
          .add(STORE_SIZE, Long.toString(storeSize))
          .add(PUSH_JOB_ID, pushJobId)
          .add(PUSH_TYPE, pushType.toString());
      String responseJson = postRequest(ControllerRoute.REQUEST_TOPIC.getPath(), params);
      return mapper.readValue(responseJson, VersionCreationResponse.class);
    } catch (Exception e){
      return handleError(
          new VeniceException("Error requesting topic for store: " + storeName, e), new VersionCreationResponse());
    }
  }

  public ControllerResponse uploadPushProperties(String storeName, int version, Properties properties) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName)
          .add(VERSION, version);
      for (Object key : properties.keySet()) {
        params.add(key.toString(), properties.getProperty(key.toString()));
      }
      String responseJson = postRequest(ControllerRoute.OFFLINE_PUSH_INFO.getPath(), params);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error uploading the push properties for store: " + storeName, e),
          new ControllerResponse());
    }
  }

  public ControllerResponse writeEndOfPush(String storeName, int version){
    try {
      QueryParams params = newParams()
          .add(NAME, storeName)
          .add(VERSION, version);
      String responseJson = postRequest(ControllerRoute.END_OF_PUSH.getPath(), params);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e){
      return handleError(
          new VeniceException("Error generating End Of Push for store: " + storeName, e), new ControllerResponse());
    }
  }

  public VersionCreationResponse emptyPush(String storeName, String pushJobId, long storeSize) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName)
          .add(PUSH_JOB_ID, pushJobId)
          .add(STORE_SIZE, Long.toString(storeSize));
      String responseJson = postRequest(ControllerRoute.EMPTY_PUSH.getPath(), params);
      return mapper.readValue(responseJson, VersionCreationResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error generating empty push for store: " + storeName, e), new VersionCreationResponse());
    }
  }

  @Deprecated
  public static VersionCreationResponse createNewStoreVersion(String urlsToFindMasterController, String clusterName, String storeName, long storeSize) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.createNewStoreVersion(storeName, storeSize);
    } catch (Exception e){
      return handleError(
          new VeniceException("Error creating version for store: " + storeName, e), new VersionCreationResponse());
    }
  }

  public NewStoreResponse createNewStore(String storeName, String owner, String keySchema, String valueSchema) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName)
          .add(OWNER, owner)
          .add(KEY_SCHEMA, keySchema)
          .add(VALUE_SCHEMA, valueSchema);
      String responseJson = postRequest(ControllerRoute.NEW_STORE.getPath(), params);
      return mapper.readValue(responseJson, NewStoreResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error creating store: " + storeName, e), new NewStoreResponse());
    }
  }

  public StoreMigrationResponse migrateStore(String storeName, String srcClusterName) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName)
          .add(CLUSTER, this.clusterName)
          .add(CLUSTER_SRC, srcClusterName);
      String responseJson = postRequest(ControllerRoute.MIGRATE_STORE.getPath(), params);
      return mapper.readValue(responseJson, StoreMigrationResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException( "Error migrating store: " + storeName
                  + " from: " + srcClusterName + " to: " + this.clusterName, e),
          new StoreMigrationResponse());
    }
  }

  public TrackableControllerResponse deleteStore(String storeName) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName);
      String responseJson = postRequest(ControllerRoute.DELETE_STORE.getPath(), queryParams);
      return mapper.readValue(responseJson, TrackableControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error deleting store : " + storeName, e),
          new TrackableControllerResponse());
    }
  }


  public VersionResponse overrideSetActiveVersion(String storeName, int version) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName)
          .add(VERSION, version);
      String responseJson = postRequest(ControllerRoute.SET_VERSION.getPath(), params);
      return mapper.readValue(responseJson, VersionResponse.class);
    } catch(Exception e){
      return handleError(new VeniceException("Error setting version.  Storename: " + storeName + " Version: " + version), new VersionResponse());
    }
  }

  public ControllerResponse killOfflinePushJob(String kafkaTopic) {
    try {
      String store = Version.parseStoreFromKafkaTopicName(kafkaTopic);
      int versionNumber = Version.parseVersionFromKafkaTopicName(kafkaTopic);

      QueryParams queryParams = newParams()
          .add(TOPIC, kafkaTopic) // TODO: remove once the controller is deployed to handle store and version instead
          .add(NAME, store)
          .add(VERSION, versionNumber);
      String responseJson = postRequest(ControllerRoute.KILL_OFFLINE_PUSH_JOB.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error killing job for topic: " + kafkaTopic + " in cluster: " + clusterName, e), new ControllerResponse());
    }
  }

  public ControllerResponse skipAdminMessage(String offset){
    try {
      QueryParams queryParams = newParams()
          .add(OFFSET, offset);
      String responseJson = postRequest(ControllerRoute.SKIP_ADMIN.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error skipping admin message in cluster: " + clusterName, e), new ControllerResponse());
    }
  }

  public JobStatusQueryResponse queryJobStatusWithRetry(String kafkaTopic, int attempts){
    return queryJobStatusWithRetry(kafkaTopic, attempts, Optional.empty());
  }

  public JobStatusQueryResponse queryJobStatusWithRetry(String kafkaTopic, int attempts, Optional<String> incrementalPushVersion){
    if (attempts < 1){
      throw new VeniceException("Querying with retries requires at least one attempt, called with " + attempts + " attempts");
    }
    int attemptsRemaining = attempts;
    JobStatusQueryResponse response = JobStatusQueryResponse.createErrorResponse("Request was not attempted");
    while (attemptsRemaining > 0){
      response = queryJobStatus(kafkaTopic, incrementalPushVersion); /* should always return a valid object */
      if (! response.isError()){
        return response;
      } else {
        attemptsRemaining--;
        logger.warn("Error querying job status: " + response.getError() + " -- Retrying " + attemptsRemaining + " more times...");
        Utils.sleep(2000);
      }
    }
    return response;
  }


  public JobStatusQueryResponse queryJobStatus(String kafkaTopic) {
    return queryJobStatus(kafkaTopic, Optional.empty());
  }

  public JobStatusQueryResponse queryJobStatus(String kafkaTopic, Optional<String> incrementalPushVersion) {
    try {
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
      int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
      QueryParams queryParams = newParams()
          .add(NAME, storeName)
          .add(VERSION, version);
      if (incrementalPushVersion.isPresent()) {
        queryParams.add(INCREMENTAL_PUSH_VERSION, incrementalPushVersion.get());
      }
      String responseJson = getRequest(ControllerRoute.JOB.getPath(), queryParams);
      return mapper.readValue(responseJson, JobStatusQueryResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying job status for topic: " + kafkaTopic, e), new JobStatusQueryResponse());
    }
  }

  public MultiStoreResponse queryStoreList() {
    try {
      String responseJson = getRequest(ControllerRoute.LIST_STORES.getPath());
      return mapper.readValue(responseJson, MultiStoreResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing store for cluster: " + clusterName, e), new MultiStoreResponse());
    }
  }

  @Deprecated
  public static MultiStoreResponse listStores(String urlsToFindMasterController, String clusterName){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.queryStoreList();
    } catch (Exception e){
      return handleError(new VeniceException("Error listing store for cluster: " + clusterName, e), new MultiStoreResponse());
    }
  }

  public MultiStoreStatusResponse listStoresStatuses() {
    try {
      String responseJson = getRequest(ControllerRoute.CLUSTER_HELATH_STORES.getPath());
      return mapper.readValue(responseJson, MultiStoreStatusResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error listing store status for cluster: " + clusterName, e),
          new MultiStoreStatusResponse());
    }
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

  private ControllerResponse enableStore(String storeName, boolean enable, String operation){
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName)
          .add(STATUS, enable)
          .add(OPERATION, operation);
      String responseJson = postRequest(ControllerRoute.ENABLE_STORE.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e){
      String msg = enable ?
          "Could not enable store for " + operation + " :" + storeName :
          "Could not disable store for" + operation + " :" + storeName;
      return handleError(new VeniceException(msg, e), new ControllerResponse());
    }
  }

  public MultiVersionResponse deleteAllVersions(String storeName) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName);
      String responseJson = postRequest(ControllerRoute.DELETE_ALL_VERSIONS.getPath(), queryParams);
      return mapper.readValue(responseJson, MultiVersionResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error deleting all version for store : " + storeName, e),
          new MultiVersionResponse());
    }
  }

  public VersionResponse deleteOldVersion(String storeName, int versionNum) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName)
          .add(VERSION, versionNum);
      String responseJson = postRequest(ControllerRoute.DELETE_OLD_VERSION.getPath(), queryParams);
      return mapper.readValue(responseJson, VersionResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error deleting version:" + versionNum + " for store : " + storeName, e),
          new VersionResponse());
    }
  }

  public NodeStatusResponse isNodeRemovable(String instanceId) {
    return singleNodeOperation(instanceId, ControllerRoute.NODE_REMOVABLE.getPath(), HttpGet.METHOD_NAME, NodeStatusResponse.class);
  }

  public ControllerResponse addNodeIntoWhiteList(String instanceId) {
    return singleNodeOperation(instanceId, ControllerRoute.WHITE_LIST_ADD_NODE.getPath(), HttpPost.METHOD_NAME, ControllerResponse.class);
  }

  public ControllerResponse removeNodeFromWhiteList(String instanceId) {
    return singleNodeOperation(instanceId, ControllerRoute.WHITE_LIST_REMOVE_NODE.getPath(), HttpPost.METHOD_NAME, ControllerResponse.class);
  }

  public ControllerResponse removeNodeFromCluster(String instanceId) {
    return singleNodeOperation(instanceId, ControllerRoute.REMOVE_NODE.getPath(), HttpPost.METHOD_NAME, ControllerResponse.class);
  }

  /**
   * Generic method to process all operations against the single node.
   */
  private <T extends ControllerResponse> T singleNodeOperation(String instanceId, String path, String method,
      Class<T> responseType) {
    try {
      QueryParams queryParams = newParams()
          .add(STORAGE_NODE_ID, instanceId);
      String responseJson = null;
      if (method.equals(HttpGet.METHOD_NAME)) {
        responseJson = getRequest(path, queryParams);
      } else if (method.equals(HttpPost.METHOD_NAME)) {
        responseJson = postRequest(path, queryParams);
      } else {
        throw new VeniceUnsupportedOperationException(method);
      }
      return mapper.readValue(responseJson, responseType);
    } catch (Exception e) {
      try {
        return handleError(new VeniceException("Could not complete the operation on the node: " + instanceId, e),
            responseType.newInstance());
      } catch (IllegalAccessException | InstantiationException reflectException) {
        String errorMsg = "Could not create response of type:" + responseType.getName();
        logger.error(errorMsg, reflectException);
        throw new VeniceException(errorMsg, reflectException);
      }
    }
  }

  public MultiNodeResponse listStorageNodes() {
    try {
      String responseJson = getRequest(ControllerRoute.LIST_NODES.getPath());
      return mapper.readValue(responseJson, MultiNodeResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing nodes", e), new MultiNodeResponse());
    }
  }

  public MultiNodesStatusResponse listInstancesStatuses() {
    try {
      String responseJson = getRequest(ControllerRoute.ClUSTER_HEALTH_INSTANCES.getPath());
      return mapper.readValue(responseJson, MultiNodesStatusResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error listing nodes", e), new MultiNodesStatusResponse());
    }
  }

  public MultiReplicaResponse listReplicas(String storeName, int version) {
    try {
      QueryParams params = newParams()
          .add(NAME, storeName)
          .add(VERSION, version);
      String responseJson = getRequest(ControllerRoute.LIST_REPLICAS.getPath(), params);
      return mapper.readValue(responseJson, MultiReplicaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for store: " + storeName + " version: " + version, e), new MultiReplicaResponse());
    }
  }

  public MultiReplicaResponse listStorageNodeReplicas(String instanceId) {
    try {
      QueryParams params = newParams()
          .add(STORAGE_NODE_ID, instanceId);
      String responseJson = getRequest(ControllerRoute.NODE_REPLICAS.getPath(), params);
      return mapper.readValue(responseJson, MultiReplicaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for storage node: " + instanceId, e), new MultiReplicaResponse());
    }
  }

  /* SCHEMA */
  public SchemaResponse getKeySchema(String storeName) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName);
      String responseJson = getRequest(ControllerRoute.GET_KEY_SCHEMA.getPath(), queryParams);
      return mapper.readValue(responseJson, SchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting key schema for store: " + storeName, e), new SchemaResponse());
    }
  }

  public SchemaResponse addValueSchema(String storeName, String valueSchemaStr) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName)
          .add(VALUE_SCHEMA, valueSchemaStr);
      String responseJson = postRequest(ControllerRoute.ADD_VALUE_SCHEMA.getPath(), queryParams);
      return mapper.readValue(responseJson, SchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error adding value schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  public PartitionResponse setStorePartitionCount(String storeName, String partitionNum) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName)
          .add(PARTITION_COUNT, partitionNum);
      String responseJson = postRequest(ControllerRoute.SET_PARTITION_COUNT.getPath(), queryParams);
      return mapper.readValue(responseJson, PartitionResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error updating partition number: " + partitionNum + " for store: " + storeName, e), new PartitionResponse());
    }
  }

  public OwnerResponse setStoreOwner(String storeName, String owner) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName)
          .add(OWNER, owner);
      String responseJson = postRequest(ControllerRoute.SET_OWNER.getPath(), queryParams);
      return mapper.readValue(responseJson, OwnerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error updating owner info: " + owner + " for store: " + storeName, e), new OwnerResponse());
    }
  }

  public ControllerResponse updateStore(String storeName, UpdateStoreQueryParams params) {
    try {
      QueryParams queryParams = addCommonParams(params)
          .add(NAME, storeName);
      String responseJson = postRequest(ControllerRoute.UPDATE_STORE.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error updating tore: " + storeName, e), new ControllerResponse());
    }
  }

  public SchemaResponse getValueSchema(String storeName, int valueSchemaId) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName)
          .add(SCHEMA_ID, valueSchemaId);
      String responseJson = getRequest(ControllerRoute.GET_VALUE_SCHEMA.getPath(), queryParams);
      return mapper.readValue(responseJson, SchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema id: " + valueSchemaId + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  public SchemaResponse getValueSchemaID(String storeName, String valueSchemaStr) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName)
          .add(VALUE_SCHEMA, valueSchemaStr);
      String responseJson = postRequest(ControllerRoute.GET_VALUE_SCHEMA_ID.getPath(), queryParams);
      return mapper.readValue(responseJson, SchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  public MultiSchemaResponse getAllValueSchema(String storeName) {
    try {
      QueryParams queryParams = newParams()
          .add(NAME, storeName);
      String responseJson = getRequest(ControllerRoute.GET_ALL_VALUE_SCHEMA.getPath(), queryParams);
      return mapper.readValue(responseJson, MultiSchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for store: " + storeName, e), new MultiSchemaResponse());
    }
  }

  public AdminCommandExecutionResponse getAdminCommandExecution(long executionId) {
    try {
      QueryParams queryParams = newParams()
          .add(EXECUTION_ID, executionId);
      String responseJson = getRequest(ControllerRoute.EXECUTION.getPath(), queryParams);
      return mapper.readValue(responseJson, AdminCommandExecutionResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error getting execution: " + executionId, e),
          new AdminCommandExecutionResponse());
    }
  }

  public LastSucceedExecutionIdResponse getLastSucceedExecutionId() {
    try {
      String responseJson = getRequest(ControllerRoute.LAST_SUCCEED_EXECUTION_ID.getPath());
      return mapper.readValue(responseJson, LastSucceedExecutionIdResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error getting the last succeed execution Id", e),
          new LastSucceedExecutionIdResponse());
    }
  }

  public ControllerResponse enableThrotting(boolean isThrottlingEnabled) {
    try {
      QueryParams queryParams = newParams()
          .add(STATUS, isThrottlingEnabled);
      String responseJson = postRequest(ControllerRoute.ENABLE_THROTTLING.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error enabling the throttling feature.", e), new ControllerResponse());
    }
  }

  public ControllerResponse enableMaxCapacityProtection(boolean isMaxCapacityProtion) {
    try {
      QueryParams queryParams = newParams()
          .add(STATUS, isMaxCapacityProtion);
      String responseJson = postRequest(ControllerRoute.ENABLE_MAX_CAPACITY_PROTECTION.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error enabling the max capacity protection feature.", e),
          new ControllerResponse());
    }
  }

  public ControllerResponse enableQuotaRebalanced(boolean isQuotaRebalanced, int expectRouterCount) {
    try {
      QueryParams queryParams = newParams()
          .add(STATUS, isQuotaRebalanced)
          .add(EXPECTED_ROUTER_COUNT, expectRouterCount);
      String responseJson = postRequest(ControllerRoute.ENABLE_QUOTA_REBALANCED.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error enabling the quota rebalanced feature.", e),
          new ControllerResponse());
    }
  }

  public RoutersClusterConfigResponse getRoutersClusterConfig() {
    try {
      String responseJson = getRequest(ControllerRoute.GET_ROUTERS_CLUSTER_CONFIG.getPath());
      return mapper.readValue(responseJson, RoutersClusterConfigResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error getting the routers cluster config.", e),
          new RoutersClusterConfigResponse());
    }
  }

  public MigrationPushStrategyResponse getMigrationPushStrategies() {
    try {
      String responseJson = getRequest(ControllerRoute.GET_ALL_MIGRATION_PUSH_STRATEGIES.getPath());
      return mapper.readValue(responseJson, MigrationPushStrategyResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error getting migration push strategies.", e),
          new MigrationPushStrategyResponse());
    }
  }

  public ControllerResponse setMigrationPushStrategy(String voldemortStoreName, String pushStrategy) {
    try {
      QueryParams queryParams = newParams()
          .add(VOLDEMORT_STORE_NAME, voldemortStoreName)
          .add(PUSH_STRATEGY, pushStrategy);
      String responseJson = getRequest(ControllerRoute.SET_MIGRATION_PUSH_STRATEGY.getPath(), queryParams);
      return mapper.readValue(responseJson, MigrationPushStrategyResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error getting migration push strategies.", e),
          new ControllerResponse());
    }
  }

  public MultiVersionStatusResponse listBootstrappingVersions(){
    try {
      String responseJson = getRequest(ControllerRoute.LIST_BOOTSTRAPPING_VERSIONS.getPath());
      return mapper.readValue(responseJson, MultiVersionStatusResponse.class);
    }catch (Exception e){
      return handleError(new VeniceException("Error listing bootstrapping versions.", e),
          new MultiVersionStatusResponse());
    }
  }

  public static D2ServiceDiscoveryResponse discoverCluster(String veniceUrls, String storeName) {
    List<String> urlList = Arrays.asList(veniceUrls.split(","));
    Exception lastException = null;
    for (String url : urlList) {
      try {
        QueryParams queryParams = new QueryParams()
        // Cluster name is not required for cluster discovery request. But could not null otherwise an exception will be
        // thrown on server side.
            .add(CLUSTER, "*")
            .add(HOSTNAME, Utils.getHostName())
            .add(NAME, storeName);
        String responseJson = getRequest(url, ControllerRoute.CLUSTER_DISCOVERY.getPath(), queryParams);
        return mapper.readValue(responseJson, D2ServiceDiscoveryResponse.class);
      } catch (Exception e) {
        try {
          logger.info("Met error to discover cluster from: " + ControllerRoute.CLUSTER_DISCOVERY.getPath().toString() + ", try "
              + ControllerRoute.CLUSTER_DISCOVERY.getPath().toString() + "/" + storeName + "...", e);
          // Because the way to get parameter is different between controller and router, in order to support query cluster
          // from both cluster and router, we send the path "/discover_cluster?storename=$storename" at first, if it does
          // not work, try "/discover_cluster/$storeName"
          String responseJson =
              getRequest(url, ControllerRoute.CLUSTER_DISCOVERY.getPath() + "/" + storeName, new QueryParams());
          return mapper.readValue(responseJson, D2ServiceDiscoveryResponse.class);
        } catch (Exception exception) {
          lastException = exception;
          continue;
        }
      }
    }
    String errorMsg = "Error discovering cluster from urls: " + veniceUrls;
    if (lastException != null) {
      lastException = new VeniceException(errorMsg, lastException);
    } else {
      lastException = new VeniceException(errorMsg);
    }
    return handleError(lastException, new D2ServiceDiscoveryResponse());
  }

  /***
   * Add all global parameters in this method. Always use a form of this method to generate
   * a new list of NameValuePair objects for making HTTP requests.
   * @return
   */
  private QueryParams newParams(){
    return addCommonParams(new QueryParams());
  }

  /**
   * Add global parameters and also set the clustername parameter to the passed in value.
   * @param clusterName
   * @return
   */
  protected QueryParams newParams(String clusterName){
    return addCommonParams(new QueryParams(), clusterName);
  }

  private QueryParams addCommonParams(QueryParams params) {
    return addCommonParams(params, clusterName);
  }

  private QueryParams addCommonParams(QueryParams params, String clusterName) {
    return params
        .add(HOSTNAME, localHostname)
        .add(CLUSTER, clusterName);
  }

  private String getRequest(String path)
      throws ExecutionException, InterruptedException {
    return getRequest(path, newParams());
  }

  private String getRequest(String path, QueryParams params)
      throws ExecutionException, InterruptedException {
    refreshControllerUrl();
    return getRequest(masterControllerUrl, path, params);
  }

  private static String getRequest(String url, String path, QueryParams params)
      throws ExecutionException, InterruptedException {
    url = url.trim();
    String queryString = URLEncodedUtils.format(params.getNameValuePairs(), StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(url + path + "?" + queryString);
    return getJsonFromHttp(get, false);
  }

  private String postRequest(String path, QueryParams params)
      throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    return postRequest(path, params, 10);
  }

  private String postRequest(String path, QueryParams params, int retriesLeftForNonMasterController)
      throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    try {
      refreshControllerUrl();
    } catch (VeniceException e) {
      if (retriesLeftForNonMasterController > 0) {
        Utils.sleep(500);
        logger.warn("Failed to refresh master controller URL. Will retry up to " + retriesLeftForNonMasterController + " more times.", e);
        return postRequest(path, params, retriesLeftForNonMasterController - 1);
      }
      throw e;
    }
    final HttpPost post = new HttpPost(masterControllerUrl + path);
    post.setEntity(new UrlEncodedFormEntity(params.getNameValuePairs()));
    try {
      boolean throwForNonMasterController = retriesLeftForNonMasterController > 0;
      return getJsonFromHttp(post, throwForNonMasterController);
    } catch (VeniceHttpException e) {
      if (e.getHttpStatusCode() == HttpConstants.SC_MISDIRECTED_REQUEST && retriesLeftForNonMasterController > 0) {
        logger.warn("Failed to reach master controller " + masterControllerUrl + ". Will retry up to " + retriesLeftForNonMasterController + " more times.", e);
        Utils.sleep(500);
        return postRequest(path, params, retriesLeftForNonMasterController - 1);
      }
      throw e;
    }
  }

  private static <R extends ControllerResponse> R handleError(Exception e, R errorResponse){
    String message = e.getMessage();
    if (e.getCause() != null) {
      message += " -- " + e.getCause().getMessage();
    }
    logger.error(message, e);
    errorResponse.setError(message);
    return errorResponse;
  }

  private static String getJsonFromHttp(HttpRequestBase httpRequest, boolean throwForNonMasterController) {
    HttpResponse response = null;
    try(CloseableHttpAsyncClient httpClient =
        HttpAsyncClients
        .custom()
        .setDefaultRequestConfig(
            RequestConfig
            .custom()
            .setSocketTimeout((int) TimeUnit.MINUTES.toMillis(ADMIN_REQUEST_TIMEOUT_MIN))
            .build())
        .build()){
      httpClient.start();
      if (!httpClient.isRunning()) {
        throw new VeniceException("httpClient is not running!");
      }
      response = httpClient.execute(httpRequest, null).get();
    } catch (Exception e) {
      String msg = "Exception making HTTP request: " + e.getMessage();
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
    if (response.getStatusLine().getStatusCode() == HttpConstants.SC_MISDIRECTED_REQUEST && throwForNonMasterController) {
      throw new VeniceHttpException(HttpConstants.SC_MISDIRECTED_REQUEST);
    }
    String responseBody;
    try (InputStream bodyStream = response.getEntity().getContent()) {
      responseBody = IOUtils.toString(bodyStream);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
    String contentType = response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue();
    if (contentType.startsWith(HttpConstants.JSON)) {
      return responseBody;
    } else { //non JSON response
      String msg = "controller returns with content-type " + contentType + ": " + responseBody;
      logger.error(msg);
      throw new VeniceException(msg);
    }
  }

  protected String getClusterName() {
    return this.clusterName;
  }
}
