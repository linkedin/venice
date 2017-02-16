package com.linkedin.venice.controllerapi;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.VeniceException;
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

  //TODO: clusterName seems to be redundant here. Since controller client is intended to have one-one mapping to clusterName.
  public StoreResponse getStore(String clusterName, String storeName) {
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
      return client.getStore(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting store: " + storeName, e), new StoreResponse());
    }
  }

  public VersionCreationResponse createNewStoreVersion(String clusterName, String storeName, long storeSize) {
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

  @Deprecated
  public static VersionCreationResponse createNewStoreVersion(String urlsToFindMasterController, String clusterName, String storeName, long storeSize) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.createNewStoreVersion(clusterName, storeName, storeSize);
    } catch (Exception e){
      return handleError(
          new VeniceException("Error creating version for store: " + storeName, e), new VersionCreationResponse());
    }
  }

  public NewStoreResponse createNewStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema) {
    try {
      List<NameValuePair> params = newParams(clusterName);
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner));
      params.add(new BasicNameValuePair(ControllerApiConstants.KEY_SCHEMA, keySchema));
      params.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchema));
      String responseJson = postRequest(ControllerRoute.NEW_STORE.getPath(), params);
      return mapper.readValue(responseJson, NewStoreResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error creating store: " + storeName, e), new NewStoreResponse());
    }
  }

  @Deprecated
  public static NewStoreResponse createNewStore(String urlsToFindMasterController, String clusterName,
      String storeName, String owner, String keySchema, String valueSchema){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.createNewStore(clusterName, storeName, owner, keySchema, valueSchema);
    } catch (Exception e){
      return handleError(new VeniceException("Error creating store: " + storeName, e), new NewStoreResponse());
    }
  }

  public VersionResponse overrideSetActiveVersion(String clusterName, String storeName, int version) {
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
      return client.overrideSetActiveVersion(clusterName, storeName, version);
    } catch(Exception e){
      return handleError(new VeniceException("Error setting version.  Storename: " + storeName + " Version: " + version), new VersionResponse());
    }
  }

  public ControllerResponse killOfflinePushJob(String clusterName, String kafkaTopic) {
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

  public ControllerResponse skipAdminMessage(String clusterName, String offset){
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.OFFSET, offset));
      String responseJson = postRequest(ControllerRoute.SKIP_ADMIN.getPath(), queryParams);
      return mapper.readValue(responseJson, ControllerResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Error skipping admin message in cluster: " + clusterName, e), new ControllerResponse());
    }
  }

  public JobStatusQueryResponse queryJobStatus(String clusterName, String kafkaTopic) {
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
      return client.queryJobStatus(clusterName, kafkaTopic);
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

  public MultiStoreResponse queryStoreList(String clusterName) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      String responseJson = getRequest(ControllerRoute.LIST_STORES.getPath(), queryParams);
      return mapper.readValue(responseJson, MultiStoreResponse.class);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying store list for cluster: " + clusterName, e), new MultiStoreResponse());
    }
  }

  @Deprecated
  public static MultiStoreResponse queryStoreList(String urlsToFindMasterController, String clusterName){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.queryStoreList(clusterName);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying store list for cluster: " + clusterName, e), new MultiStoreResponse());
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

  public ControllerResponse enableStoreWrites(String clusterName, String storeName, boolean enable) {
    return enableStore(clusterName, storeName, enable, ControllerApiConstants.WRITE_OPERATION);
  }

  public ControllerResponse enableStoreReads(String clusterName, String storeName, boolean enable) {
    return enableStore(clusterName, storeName, enable, ControllerApiConstants.READ_OPERATION);
  }

  public ControllerResponse enableStoreReadWrites(String clusterName, String storeName, boolean enable) {
    return enableStore(clusterName, storeName, enable, ControllerApiConstants.READ_WRITE_OPERATION);
  }

  private ControllerResponse enableStore(String clusterName, String storeName, boolean enable, String operation){
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
      return client.enableStoreWrites(clusterName, storeName, enable);
    } catch (Exception e){
      String msg = enable ?
          "Could not enable store: " + storeName :
          "Could not disable store: " + storeName;
      return handleError(new VeniceException(msg, e), new ControllerResponse());
    }
  }

  public MultiVersionResponse deleteAllVersions(String clusterName, String storeName) {
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


  public NodeStatusResponse isNodeRemovable(String clusterName, String instanceId) {
    try {
      List<NameValuePair> queryParams = newParams(clusterName);
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.STORAGE_NODE_ID, instanceId));
      String responseJson = getRequest(ControllerRoute.NODE_REMOVABLE.getPath(), queryParams);
      return mapper.readValue(responseJson, NodeStatusResponse.class);
    } catch (Exception e) {
      return handleError(new VeniceException("Could not identify if node: " + instanceId + " is removable", e), new NodeStatusResponse());
    }
  }

  @Deprecated
  public static ControllerResponse isNodeRemovable(String urlsToFindMasterController, String clusterName, String instanceId){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.isNodeRemovable(clusterName, instanceId);
    } catch (Exception e){
      return handleError(new VeniceException("Could not identify if node: " + instanceId + " is removable", e), new ControllerResponse());
    }
  }

  public MultiNodeResponse listStorageNodes(String clusterName) {
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
      return client.listStorageNodes(clusterName);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing nodes", e), new MultiNodeResponse());
    }
  }

  public MultiReplicaResponse listReplicas(String clusterName, String storeName, int version) {
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
      return client.listReplicas(clusterName, storeName, version);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for store: " + storeName + " version: " + version, e), new MultiReplicaResponse());
    }
  }

  public MultiReplicaResponse listStorageNodeReplicas(String clusterName, String instanceId) {
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
      return client.listStorageNodeReplicas(clusterName, instanceId);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for storage node: " + instanceId, e), new MultiReplicaResponse());
    }
  }

  /* SCHEMA */
  public SchemaResponse getKeySchema(String clusterName, String storeName) {
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
      return client.getKeySchema(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting key schema for store: " + storeName, e), new SchemaResponse());
    }
  }

  public SchemaResponse addValueSchema(String clusterName, String storeName, String valueSchemaStr) {
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
      return client.addValueSchema(clusterName, storeName, valueSchemaStr);
    } catch (Exception e){
      return handleError(new VeniceException("Error adding value schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  public SchemaResponse getValueSchema(String clusterName, String storeName, int valueSchemaId) {
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
      return client.getValueSchema(clusterName, storeName, valueSchemaId);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema id: " + valueSchemaId + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  public SchemaResponse getValueSchemaID(String clusterName, String storeName, String valueSchemaStr) {
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
      return client.getValueSchemaID(clusterName, storeName, valueSchemaStr);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  public MultiSchemaResponse getAllValueSchema(String clusterName, String storeName) {
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
      return client.getAllValueSchema(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for store: " + storeName, e), new MultiSchemaResponse());
    }
  }

  public AdminCommandExecutionResponse getAdminCommandExecution(String clusterName, long executionId) {
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

  public LastSucceedExecutionIdResponse getLastSucceedExecutionId(String clusterName) {
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
            .setSocketTimeout((int) TimeUnit.MINUTES.toMillis(3))
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
