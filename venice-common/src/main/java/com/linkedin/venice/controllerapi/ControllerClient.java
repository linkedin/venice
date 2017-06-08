package com.linkedin.venice.controllerapi;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

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
        List<NameValuePair> params = newParams(clusterName);
        String responseBody = getRequest(url, ControllerRoute.MASTER_CONTROLLER.getPath(), params);
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
    throw new VeniceException("Could not get controller url from urls: " + urlsToFindMasterController, lastException);
  }

  public StoreResponse getStore(String storeName) {
    try {
      List<NameValuePair> params = newParams(clusterName);
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
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

  /**
   * Use {@link #requestTopicForWrites(String, long, com.linkedin.venice.controllerapi.ControllerApiConstants.PushType, String) requestTopicForWrites} instead
   * @param storeName
   * @param storeSize
   * @return
   */
  @Deprecated //Marked deprecated, but we should keep using this for H2V until we can do push ID based idempotent topic requests
  public VersionCreationResponse createNewStoreVersion(String storeName, long storeSize) {
    try {
      List<NameValuePair> params = newParams(clusterName);
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(storeSize)));
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
  public VersionCreationResponse requestTopicForWrites(String storeName, long storeSize, ControllerApiConstants.PushType pushType, String pushJobId) {
    try {
      List<NameValuePair> params = newParams(clusterName);
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(storeSize)));
      params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_JOB_ID, pushJobId));
      params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_TYPE, pushType.toString()));
      String responseJson = postRequest(ControllerRoute.REQUEST_TOPIC.getPath(), params);
      return mapper.readValue(responseJson, VersionCreationResponse.class);
    } catch (Exception e){
      return handleError(
          new VeniceException("Error requesting topic for store: " + storeName, e), new VersionCreationResponse());
    }
  }

  public ControllerResponse writeEndOfPush(String storeName, int version){
    try {
      List<NameValuePair> params = newParams(clusterName);
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      params.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
      String responseJson = postRequest(ControllerRoute.END_OF_PUSH.getPath(), params);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e){
      return handleError(
          new VeniceException("Error generating End Of Push for store: " + storeName, e), new ControllerResponse());
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

  public NewStoreResponse createNewStore(String storeName, String owner, String principles, String keySchema,
      String valueSchema) {
    return createNewStore(storeName, owner, Utils.parseCommaSeparatedStringToList(principles), keySchema, valueSchema);
  }

  protected NewStoreResponse createNewStore(String storeName, String owner, List<String> principles, String keySchema, String valueSchema) {
    try {
      List<NameValuePair> params = newParams(clusterName);
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner));
      params.add(new BasicNameValuePair(ControllerApiConstants.KEY_SCHEMA, keySchema));
      params.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchema));
      params.add(new BasicNameValuePair(ControllerApiConstants.PRINCIPLES, String.join(",", principles)));
      String responseJson = postRequest(ControllerRoute.NEW_STORE.getPath(), params);
      return mapper.readValue(responseJson, NewStoreResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error creating store: " + storeName, e), new NewStoreResponse());
    }
  }


  public VersionResponse overrideSetActiveVersion(String storeName, int version) {
    try {
      List<NameValuePair> params = newParams(clusterName);
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      params.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
      String responseJson = postRequest(ControllerRoute.SET_VERSION.getPath(), params);
      return mapper.readValue(responseJson, VersionResponse.class);
    } catch(Exception e){
      return handleError(new VeniceException("Error setting version.  Storename: " + storeName + " Version: " + version), new VersionResponse());
    }
  }

  @Deprecated
  public static VersionResponse overrideSetActiveVersion(String urlsToFindMasterController, String clusterName, String storeName, int version){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.overrideSetActiveVersion(storeName, version);
    } catch(Exception e){
      return handleError(new VeniceException("Error setting version.  Storename: " + storeName + " Version: " + version), new VersionResponse());
    }
  }

  public ControllerResponse killOfflinePushJob(String kafkaTopic) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.TOPIC, kafkaTopic)); // TODO: remove once the controller is deployed to handle store and version instead
      String store = Version.parseStoreFromKafkaTopicName(kafkaTopic);
      int versionNumber = Version.parseVersionFromKafkaTopicName(kafkaTopic);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, store));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(versionNumber)));
      String responseJson = postRequest(ControllerRoute.KILL_OFFLINE_PUSH_JOB.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error killing job for topic: " + kafkaTopic + " in cluster: " + clusterName, e), new ControllerResponse());
    }
  }

  public ControllerResponse skipAdminMessage(String offset){
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.OFFSET, offset));
      String responseJson = postRequest(ControllerRoute.SKIP_ADMIN.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error skipping admin message in cluster: " + clusterName, e), new ControllerResponse());
    }
  }

  public JobStatusQueryResponse queryJobStatus(String kafkaTopic) {
    try {
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
      int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
      String responseJson = getRequest(ControllerRoute.JOB.getPath(), queryParams);
      return mapper.readValue(responseJson, JobStatusQueryResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying job status for topic: " + kafkaTopic, e), new JobStatusQueryResponse());
    }
  }

  @Deprecated
  public static JobStatusQueryResponse queryJobStatus(String urlsToFindMasterController, String clusterName, String kafkaTopic){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.queryJobStatus(kafkaTopic);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying job status for topic: " + kafkaTopic, e), new JobStatusQueryResponse());
    }
  }

  //TODO: make this a class method
  public static JobStatusQueryResponse queryJobStatusWithRetry(String urlsToFindMasterController, String clusterName, String kafkaTopic, int attempts){
    if (attempts < 1){
      throw new VeniceException("Querying with retries requires at least one attempt, called with " + attempts + " attempts");
    }
    int attemptsRemaining = attempts;
    JobStatusQueryResponse response = JobStatusQueryResponse.createErrorResponse("Request was not attempted");
    while (attemptsRemaining > 0){
      response = queryJobStatus(urlsToFindMasterController, clusterName, kafkaTopic); /* should always return a valid object */
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

  public MultiStoreResponse queryStoreList() {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      String responseJson = getRequest(ControllerRoute.LIST_STORES.getPath(), queryParams);
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

  public MultiStoreStatusResponse listStoresStatuses(String clusterName) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      String responseJson = getRequest(ControllerRoute.CLUSTER_HELATH_STORES.getPath(), queryParams);
      return mapper.readValue(responseJson, MultiStoreStatusResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error listing store status for cluster: " + clusterName, e),
          new MultiStoreStatusResponse());
    }
  }

  @Deprecated // use getStore
  private VersionResponse queryCurrentVersion(String clusterName, String storeName)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String responseJson = getRequest(ControllerRoute.CURRENT_VERSION.getPath(), queryParams);
    return mapper.readValue(responseJson, VersionResponse.class);
  }

  @Deprecated // use getStore
  public static VersionResponse queryCurrentVersion(String urlsToFindMasterController, String clusterName, String storeName){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.queryCurrentVersion(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying current version for store: " + storeName, e), new VersionResponse());
    }
  }

  @Deprecated // use getStore
  private MultiVersionResponse queryActiveVersions(String clusterName, String storeName)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String responseJson = getRequest(ControllerRoute.ACTIVE_VERSIONS.getPath(), queryParams);
    return mapper.readValue(responseJson, MultiVersionResponse.class);
  }

  @Deprecated // use getStore
  public static MultiVersionResponse queryActiveVersions(String urlsToFindMasterController, String clusterName, String storeName){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.queryActiveVersions(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying active version for store: " + storeName, e), new MultiVersionResponse());
    }
  }

  public ControllerResponse enableStoreWrites(String storeName, boolean enable) {
    return enableStore(storeName, enable, ControllerApiConstants.WRITE_OPERATION);
  }

  public ControllerResponse enableStoreReads(String storeName, boolean enable) {
    return enableStore(storeName, enable, ControllerApiConstants.READ_OPERATION);
  }

  public ControllerResponse enableStoreReadWrites(String storeName, boolean enable) {
    return enableStore(storeName, enable, ControllerApiConstants.READ_WRITE_OPERATION);
  }

  private ControllerResponse enableStore(String storeName, boolean enable, String operation){
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.STATUS, Boolean.toString(enable)));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.OPERATION, operation));
      String responseJson = postRequest(ControllerRoute.ENABLE_STORE.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e){
      String msg = enable ?
          "Could not enable store for " + operation + " :" + storeName :
          "Could not disable store for" + operation + " :" + storeName;
      return handleError(new VeniceException(msg, e), new ControllerResponse());
    }
  }

  @Deprecated
  public static ControllerResponse enableStoreWrites(String urlsToFindMasterController, String clusterName, String storeName, boolean enable){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.enableStoreWrites(storeName, enable);
    } catch (Exception e){
      String msg = enable ?
          "Could not enable store: " + storeName :
          "Could not disable store: " + storeName;
      return handleError(new VeniceException(msg, e), new ControllerResponse());
    }
  }

  public MultiVersionResponse deleteAllVersions(String storeName) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      String responseJson = postRequest(ControllerRoute.DELETE_ALL_VERSIONS.getPath(), queryParams);
      return mapper.readValue(responseJson, MultiVersionResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error deleting all version for store : " + storeName, e),
          new MultiVersionResponse());
    }
  }

  public NodeStatusResponse isNodeRemovable(String instanceId) {
    return singleNodeOperation(instanceId, ControllerRoute.NODE_REMOVABLE.getPath(), HttpGet.METHOD_NAME, NodeStatusResponse.class);
  }

  @Deprecated
  public static ControllerResponse isNodeRemovable(String urlsToFindMasterController, String clusterName, String instanceId){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.isNodeRemovable(instanceId);
    } catch (Exception e){
      return handleError(new VeniceException("Could not identify if node: " + instanceId + " is removable", e), new ControllerResponse());
    }
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
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.STORAGE_NODE_ID, instanceId));
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
      List<NameValuePair> queryParams = newParams(clusterName);
      String responseJson = getRequest(ControllerRoute.LIST_NODES.getPath(), queryParams);
      return mapper.readValue(responseJson, MultiNodeResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing nodes", e), new MultiNodeResponse());
    }
  }

  @Deprecated
  public static MultiNodeResponse listStorageNodes(String urlsToFindMasterController, String clusterName){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.listStorageNodes();
    } catch (Exception e){
      return handleError(new VeniceException("Error listing nodes", e), new MultiNodeResponse());
    }
  }

  public MultiNodesStatusResponse listInstancesStatuses() {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      String responseJson = getRequest(ControllerRoute.ClUSTER_HEALTH_INSTANCES.getPath(), queryParams);
      return mapper.readValue(responseJson, MultiNodesStatusResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error listing nodes", e), new MultiNodesStatusResponse());
    }
  }

  @Deprecated
  public static MultiNodesStatusResponse listInstancesStatuses(String urlsToFindMasterController,
      String clusterName) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)) {
      return client.listInstancesStatuses();
    } catch (Exception e) {
      return handleError(new VeniceException("Error listing nodes", e), new MultiNodesStatusResponse());
    }
  }

  public MultiReplicaResponse listReplicas(String storeName, int version) {
    try {
      List<NameValuePair> params = newParams(clusterName);
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      params.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
      String responseJson = getRequest(ControllerRoute.LIST_REPLICAS.getPath(), params);
      return mapper.readValue(responseJson, MultiReplicaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for store: " + storeName + " version: " + version, e), new MultiReplicaResponse());
    }
  }

  @Deprecated
  public static MultiReplicaResponse listReplicas(String urlsToFindMasterController, String clusterName, String storeName, int version){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.listReplicas(storeName, version);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for store: " + storeName + " version: " + version, e), new MultiReplicaResponse());
    }
  }

  public MultiReplicaResponse listStorageNodeReplicas(String instanceId) {
    try {
      List<NameValuePair> params = newParams(clusterName);
      params.add(new BasicNameValuePair(ControllerApiConstants.STORAGE_NODE_ID, instanceId));
      String responseJson = getRequest(ControllerRoute.NODE_REPLICAS.getPath(), params);
      return mapper.readValue(responseJson, MultiReplicaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for storage node: " + instanceId, e), new MultiReplicaResponse());
    }
  }

  @Deprecated
  public static MultiReplicaResponse listStorageNodeReplicas(String urlsToFindMasterController, String clusterName, String instanceId){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.listStorageNodeReplicas(instanceId);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for storage node: " + instanceId, e), new MultiReplicaResponse());
    }
  }

  /* SCHEMA */
  public SchemaResponse getKeySchema(String storeName) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      String responseJson = getRequest(ControllerRoute.GET_KEY_SCHEMA.getPath(), queryParams);
      return mapper.readValue(responseJson, SchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting key schema for store: " + storeName, e), new SchemaResponse());
    }
  }

  @Deprecated
  public static SchemaResponse getKeySchema(String urlsToFindMasterController, String clusterName, String storeName) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.getKeySchema(storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting key schema for store: " + storeName, e), new SchemaResponse());
    }
  }

  public SchemaResponse addValueSchema(String storeName, String valueSchemaStr) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchemaStr));
      String responseJson = postRequest(ControllerRoute.ADD_VALUE_SCHEMA.getPath(), queryParams);
      return mapper.readValue(responseJson, SchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error adding value schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  @Deprecated
  public static SchemaResponse addValueSchema(String urlsToFindMasterController, String clusterName, String storeName, String valueSchemaStr) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.addValueSchema(storeName, valueSchemaStr);
    } catch (Exception e){
      return handleError(new VeniceException("Error adding value schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  public PartitionResponse setStorePartitionCount(String storeName, String partitionNum) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.PARTITION_COUNT, partitionNum));
      String responseJson = postRequest(ControllerRoute.SET_PARTITION_COUNT.getPath(), queryParams);
      return mapper.readValue(responseJson, PartitionResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error updating partition number: " + partitionNum + " for store: " + storeName, e), new PartitionResponse());
    }
  }

  public OwnerResponse setStoreOwner(String storeName, String owner) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner));
      String responseJson = postRequest(ControllerRoute.SET_OWNER.getPath(), queryParams);
      return mapper.readValue(responseJson, OwnerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error updating owner info: " + owner + " for store: " + storeName, e), new OwnerResponse());
    }
  }

  public ControllerResponse updateStore(String storeName, Optional<String> owner, Optional<String> principles,
      Optional<Integer> partitionNum, Optional<Integer> currentVersion, Optional<Boolean> enableReads,
      Optional<Boolean> enableWrites) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      if (owner.isPresent()) {
        queryParams.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner.get()));
      }
      if (principles.isPresent()) {
        queryParams.add(new BasicNameValuePair(ControllerApiConstants.PRINCIPLES, principles.get()));
      }
      if (partitionNum.isPresent()) {
        queryParams.add(new BasicNameValuePair(ControllerApiConstants.PARTITION_COUNT, partitionNum.get().toString()));
      }
      if (currentVersion.isPresent()) {
        queryParams.add(new BasicNameValuePair(ControllerApiConstants.VERSION, currentVersion.get().toString()));
      }
      if (enableReads.isPresent()) {
        queryParams.add(new BasicNameValuePair(ControllerApiConstants.ENABLE_READS, enableReads.get().toString()));
      }
      if (enableWrites.isPresent()) {
        queryParams.add(new BasicNameValuePair(ControllerApiConstants.ENABLE_WRITES, enableWrites.get().toString()));
      }
      String responseJson = postRequest(ControllerRoute.UPDATE_STORE.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error updating tore: " + storeName, e), new ControllerResponse());
    }
  }

  public SchemaResponse getValueSchema(String storeName, int valueSchemaId) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.SCHEMA_ID, Integer.toString(valueSchemaId)));
      String responseJson = getRequest(ControllerRoute.GET_VALUE_SCHEMA.getPath(), queryParams);
      return mapper.readValue(responseJson, SchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema id: " + valueSchemaId + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  @Deprecated
  public static SchemaResponse getValueSchema(String urlsToFindMasterController, String clusterName, String storeName, int valueSchemaId) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.getValueSchema(storeName, valueSchemaId);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema id: " + valueSchemaId + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  public SchemaResponse getValueSchemaID(String storeName, String valueSchemaStr) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchemaStr));
      String responseJson = postRequest(ControllerRoute.GET_VALUE_SCHEMA_ID.getPath(), queryParams);
      return mapper.readValue(responseJson, SchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  @Deprecated
  public static SchemaResponse getValueSchemaID(String urlsToFindMasterController, String clusterName, String storeName, String valueSchemaStr) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.getValueSchemaID(storeName, valueSchemaStr);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  public MultiSchemaResponse getAllValueSchema(String storeName) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      String responseJson = getRequest(ControllerRoute.GET_ALL_VALUE_SCHEMA.getPath(), queryParams);
      return mapper.readValue(responseJson, MultiSchemaResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for store: " + storeName, e), new MultiSchemaResponse());
    }
  }

  @Deprecated
  public static MultiSchemaResponse getAllValueSchema(String urlsToFindMasterController, String clusterName, String storeName) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.getAllValueSchema(storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for store: " + storeName, e), new MultiSchemaResponse());
    }
  }

  public AdminCommandExecutionResponse getAdminCommandExecution(long executionId) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.EXECUTION_ID, String.valueOf(executionId)));
      String responseJson = getRequest(ControllerRoute.EXECUTION.getPath(), queryParams);
      return mapper.readValue(responseJson, AdminCommandExecutionResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error getting execution: " + executionId, e),
          new AdminCommandExecutionResponse());
    }
  }

  public LastSucceedExecutionIdResponse getLastSucceedExecutionId() {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      String responseJson = getRequest(ControllerRoute.LAST_SUCCEED_EXECUTION_ID.getPath(), queryParams);
      return mapper.readValue(responseJson, LastSucceedExecutionIdResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error getting the last succeed execution Id", e),
          new LastSucceedExecutionIdResponse());
    }
  }

  /***
   * Add all global parameters in this method. Always use a form of this method to generate
   * a new list of NameValuePair objects for making HTTP requests.
   * @return
   */
  private List<NameValuePair> newParams(){
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(ControllerApiConstants.HOSTNAME, localHostname));
    return params;
  }

  /**
   * Add global parameters and also set the clustername parameter to the passed in value.
   * @param clusterName
   * @return
   */
  protected List<NameValuePair> newParams(String clusterName){
    List<NameValuePair> params = newParams();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    return params;
  }

  private String getRequest(String path, List<NameValuePair> params)
      throws ExecutionException, InterruptedException {
    refreshControllerUrl();
    return getRequest(masterControllerUrl, path, params);

  }

  private String getRequest(String url, String path, List<NameValuePair> params)
      throws ExecutionException, InterruptedException {
    url = url.trim();
    String queryString = URLEncodedUtils.format(params, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(url + path + "?" + queryString);
    return getJsonFromHttp(get);
  }

  private String postRequest(String path, List<NameValuePair> params)
      throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    refreshControllerUrl();
    final HttpPost post = new HttpPost(masterControllerUrl + path);
    post.setEntity(new UrlEncodedFormEntity(params));
    return getJsonFromHttp(post);
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

  private static String getJsonFromHttp(HttpRequestBase httpRequest) {
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
      response = httpClient.execute(httpRequest, null).get();
    } catch (Exception e) {
      String msg = "Exception making HTTP request: " + e.getMessage();
      logger.error(msg, e);
      throw new VeniceException(msg, e);
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
