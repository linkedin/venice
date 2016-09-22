package com.linkedin.venice.controllerapi;

import com.linkedin.venice.HttpConstants;
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
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

public class ControllerClient implements Closeable {
  private final CloseableHttpAsyncClient client;
  private String masterControllerUrl;
  private String clusterName;
  private String urlsToFindMasterController;
  private String localHostname;

  private final static ObjectMapper mapper = new ObjectMapper();
  private final static Logger logger = Logger.getLogger(ControllerClient.class);

  /**
   * It creates a thread for sending Http Requests.
   *
   * @param urlsToFindMasterController comma-delimited urls to find master controller.
   */
  private ControllerClient(String clusterName, String urlsToFindMasterController) throws IOException {
    client = HttpAsyncClients.createDefault();
    client.start();
    if(Utils.isNullOrEmpty(urlsToFindMasterController)) {
      throw new VeniceException("urlsToFindMasterController: "+ urlsToFindMasterController +" is not valid");
    }
    this.clusterName = clusterName;
    this.urlsToFindMasterController = urlsToFindMasterController;
    this.localHostname = Utils.getHostName();
    if (logger.isDebugEnabled()) {
      logger.debug("Parsed hostname as: " + localHostname);
    }
    try {
      refreshControllerUrl();
    } catch (Exception e) {
      // If we don't close http client here, there is no way to release the resources associated with it
      // since ControllerClient instance won't be constructed successfully.
      client.close();
      logger.info("Got exception during refreshControllerUrl", e);
      throw e;
    }
  }

  private void refreshControllerUrl(){
    String controllerUrl = getMasterControllerUrl(urlsToFindMasterController);
    if (controllerUrl.endsWith("/")){
      this.masterControllerUrl = controllerUrl.substring(0, controllerUrl.length()-1);
    } else {
      this.masterControllerUrl = controllerUrl;
    }
    logger.debug("Identified controller URL: " + this.masterControllerUrl + " from url: " + urlsToFindMasterController);
  }

  /**
   * If close is not called, a thread is leaked
   */
  @Override
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      String msg = "Error closing the controller client for " + masterControllerUrl;
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  private String getMasterControllerUrl(String urlsToFindMasterController){
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

  private StoreResponse getStore(String clusterName, String storeName)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> params = newParams(clusterName);
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String responseJson = getRequest(ControllerRoute.STORE.getPath(), params);
    return mapper.readValue(responseJson, StoreResponse.class);
  }

  public static StoreResponse getStore(String urlsToFindMasterController, String clusterName, String storeName){
    try (ControllerClient client = new ControllerClient(clusterName,urlsToFindMasterController)){
      return client.getStore(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting store: " + storeName, e), new StoreResponse());
    }
  }

  private VersionCreationResponse createNewStoreVersion(String clusterName, String storeName, long storeSize)
      throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> params = newParams(clusterName);
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(storeSize)));
    String responseJson = postRequest(ControllerRoute.CREATE_VERSION.getPath(), params);
    return mapper.readValue(responseJson, VersionCreationResponse.class);
  }

  public static VersionCreationResponse createNewStoreVersion(String urlsToFindMasterController, String clusterName, String storeName, long storeSize) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.createNewStoreVersion(clusterName, storeName, storeSize);
    } catch (Exception e){
      return handleError(
          new VeniceException("Error creating version for store: " + storeName, e), new VersionCreationResponse());
    }
  }

  private NewStoreResponse createNewStore(String clusterName, String storeName, String owner,
                                          String keySchema, String valueSchema)
      throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> params = newParams(clusterName);
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner));
    params.add(new BasicNameValuePair(ControllerApiConstants.KEY_SCHEMA, keySchema));
    params.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchema));
    String responseJson = postRequest(ControllerRoute.NEW_STORE.getPath(), params);
    return mapper.readValue(responseJson, NewStoreResponse.class);
  }

  public static NewStoreResponse createNewStore(String urlsToFindMasterController, String clusterName,
      String storeName, String owner, String keySchema, String valueSchema){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.createNewStore(clusterName, storeName, owner, keySchema, valueSchema);
    } catch (Exception e){
      return handleError(new VeniceException("Error creating store: " + storeName, e), new NewStoreResponse());
    }
  }

  private VersionResponse overrideSetActiveVersion(String clusterName, String storeName, int version)
      throws InterruptedException, IOException, ExecutionException {
    List<NameValuePair> params = newParams(clusterName);
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
    String responseJson = postRequest(ControllerRoute.SET_VERSION.getPath(), params);
    return mapper.readValue(responseJson, VersionResponse.class);
  }

  public static VersionResponse overrideSetActiveVersion(String urlsToFindMasterController, String clusterName, String storeName, int version){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.overrideSetActiveVersion(clusterName, storeName, version);
    } catch(Exception e){
      return handleError(new VeniceException("Error setting version.  Storename: " + storeName + " Version: " + version), new VersionResponse());
    }
  }

  private JobStatusQueryResponse queryJobStatus(String clusterName, String kafkaTopic)
      throws ExecutionException, InterruptedException, IOException {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
    String responseJson = getRequest(ControllerRoute.JOB.getPath(), queryParams);
    return mapper.readValue(responseJson, JobStatusQueryResponse.class);
  }

  public static JobStatusQueryResponse queryJobStatus(String urlsToFindMasterController, String clusterName, String kafkaTopic){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.queryJobStatus(clusterName, kafkaTopic);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying job status for topic: " + kafkaTopic, e), new JobStatusQueryResponse());
    }
  }

  public static JobStatusQueryResponse queryJobStatusWithRetry(String urlsToFindMasterController, String clusterName, String kafkaTopic, int attempts){
    if (attempts < 1){
      throw new VeniceException("Querying with retries requires at least one attempt, called with " + attempts + " attempts");
    }
    int attemptsRemaining = attempts;
    JobStatusQueryResponse response = JobStatusQueryResponse.createErrorResponse("Request was not attempted");
    while (attemptsRemaining > 0){
      response = queryJobStatus(urlsToFindMasterController, clusterName, kafkaTopic); /* should allways return a valid object */
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

  private MultiStoreResponse queryStoreList(String clusterName)
      throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams(clusterName);
    String responseJson = getRequest(ControllerRoute.LIST_STORES.getPath(), queryParams);
    return mapper.readValue(responseJson, MultiStoreResponse.class);
  }

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

  private ControllerResponse setPauseStatus(String clusterName, String storeName, boolean pause)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.STATUS, Boolean.toString(pause)));
    String responseJson = postRequest(ControllerRoute.PAUSE_STORE.getPath(), queryParams);
    return mapper.readValue(responseJson, ControllerResponse.class);
  }

  public static ControllerResponse setPauseStatus(String urlsToFindMasterController, String clusterName, String storeName, boolean pause){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.setPauseStatus(clusterName, storeName, pause);
    } catch (Exception e){
      String msg = pause ?
          "Could not pause store: " + storeName :
          "Could not resume store: " + storeName;
      return handleError(new VeniceException(msg, e), new ControllerResponse());
    }
  }

  private ControllerResponse isNodeRemovable(String clusterName, String instanceId)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.STORAGE_NODE_ID, instanceId));
    String responseJson = getRequest(ControllerRoute.NODE_REMOVABLE.getPath(), queryParams);
    return mapper.readValue(responseJson, ControllerResponse.class);
  }

  public static ControllerResponse isNodeRemovable(String urlsToFindMasterController, String clusterName, String instanceId){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.isNodeRemovable(clusterName, instanceId);
    } catch (Exception e){
      return handleError(new VeniceException("Could not identify if node: " + instanceId + " is removable", e), new ControllerResponse());
    }
  }

  private MultiNodeResponse listStorageNodes(String clusterName)
      throws InterruptedException, IOException, ExecutionException {
    List<NameValuePair> queryParams = newParams(clusterName);
    String responseJson = getRequest(ControllerRoute.LIST_NODES.getPath(), queryParams);
    return mapper.readValue(responseJson, MultiNodeResponse.class);
  }

  public static MultiNodeResponse listStorageNodes(String urlsToFindMasterController, String clusterName){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.listStorageNodes(clusterName);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing nodes", e), new MultiNodeResponse());
    }
  }

  private MultiReplicaResponse listReplicas(String clusterName, String storeName, int version)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> params = newParams(clusterName);
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
    String responseJson = getRequest(ControllerRoute.LIST_REPLICAS.getPath(), params);
    return mapper.readValue(responseJson, MultiReplicaResponse.class);
  }

  public static MultiReplicaResponse listReplicas(String urlsToFindMasterController, String clusterName, String storeName, int version){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.listReplicas(clusterName, storeName, version);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for store: " + storeName + " version: " + version, e), new MultiReplicaResponse());
    }
  }

  private MultiReplicaResponse listStorageNodeReplicas(String clusterName, String instanceId)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> params = newParams(clusterName);
    params.add(new BasicNameValuePair(ControllerApiConstants.STORAGE_NODE_ID, instanceId));
    String responseJson = getRequest(ControllerRoute.NODE_REPLICAS.getPath(), params);
    return mapper.readValue(responseJson, MultiReplicaResponse.class);
  }

  public static MultiReplicaResponse listStorageNodeReplicas(String urlsToFindMasterController, String clusterName, String instanceId){
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.listStorageNodeReplicas(clusterName, instanceId);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for storage node: " + instanceId, e), new MultiReplicaResponse());
    }
  }

  /* SCHEMA */
  private SchemaResponse getKeySchema(String clusterName, String storeName) throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String responseJson = getRequest(ControllerRoute.GET_KEY_SCHEMA.getPath(), queryParams);
    return mapper.readValue(responseJson, SchemaResponse.class);
  }

  public static SchemaResponse getKeySchema(String urlsToFindMasterController, String clusterName, String storeName) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.getKeySchema(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting key schema for store: " + storeName, e), new SchemaResponse());
    }
  }

  private SchemaResponse addValueSchema(String clusterName, String storeName, String valueSchemaStr) throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchemaStr));
    String responseJson = postRequest(ControllerRoute.ADD_VALUE_SCHEMA.getPath(), queryParams);
    return mapper.readValue(responseJson, SchemaResponse.class);
  }

  public static SchemaResponse addValueSchema(String urlsToFindMasterController, String clusterName, String storeName, String valueSchemaStr) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.addValueSchema(clusterName, storeName, valueSchemaStr);
    } catch (Exception e){
      return handleError(new VeniceException("Error adding value schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  private SchemaResponse getValueSchema(String clusterName, String storeName, int valueSchemaId) throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.SCHEMA_ID, Integer.toString(valueSchemaId)));
    String responseJson = getRequest(ControllerRoute.GET_VALUE_SCHEMA.getPath(), queryParams);
    return mapper.readValue(responseJson, SchemaResponse.class);
  }

  public static SchemaResponse getValueSchema(String urlsToFindMasterController, String clusterName, String storeName, int valueSchemaId) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.getValueSchema(clusterName, storeName, valueSchemaId);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema id: " + valueSchemaId + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  private SchemaResponse getValueSchemaID(String clusterName, String storeName, String valueSchemaStr) throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchemaStr));
    String responseJson = postRequest(ControllerRoute.GET_VALUE_SCHEMA_ID.getPath(), queryParams);
    return mapper.readValue(responseJson, SchemaResponse.class);
  }

  public static SchemaResponse getValueSchemaID(String urlsToFindMasterController, String clusterName, String storeName, String valueSchemaStr) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.getValueSchemaID(clusterName, storeName, valueSchemaStr);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  private MultiSchemaResponse getAllValueSchema(String clusterName, String storeName) throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams(clusterName);
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String responseJson = getRequest(ControllerRoute.GET_ALL_VALUE_SCHEMA.getPath(), queryParams);
    return mapper.readValue(responseJson, MultiSchemaResponse.class);
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
  private List<NameValuePair> newParams(String clusterName){
    List<NameValuePair> params = newParams();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    return params;
  }

  private String getRequest(String path, List<NameValuePair> params)
      throws ExecutionException, InterruptedException {
    return getRequest(masterControllerUrl, path, params);
  }

  private String getRequest(String url, String path, List<NameValuePair> params)
      throws ExecutionException, InterruptedException {
    String queryString = URLEncodedUtils.format(params, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(url + path + "?" + queryString);
    HttpResponse response = client.execute(get, null).get();
    return getJsonFromHttpResponse(response);
  }

  private String postRequest(String path, List<NameValuePair> params)
      throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    final HttpPost post = new HttpPost(masterControllerUrl + path);
    post.setEntity(new UrlEncodedFormEntity(params));
    HttpResponse response = client.execute(post, null).get();
    return getJsonFromHttpResponse(response);
  }

  public static MultiSchemaResponse getAllValueSchema(String urlsToFindMasterController, String clusterName, String storeName) {
    try (ControllerClient client = new ControllerClient(clusterName, urlsToFindMasterController)){
      return client.getAllValueSchema(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for store: " + storeName, e), new MultiSchemaResponse());
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

  private static String getJsonFromHttpResponse(HttpResponse response){
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
}
