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
import org.apache.http.HttpStatus;
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
  private String controllerUrl;
  private String routerUrls;
  private String localHostname;

  private final static ObjectMapper mapper = new ObjectMapper();
  private final static Logger logger = Logger.getLogger(ControllerClient.class);

  /**
   * It creates a thread for sending Http Requests.
   *
   * @param routerUrls comma-delimeted urls for the Venice Routers.
   */
  private ControllerClient(String routerUrls){
    client = HttpAsyncClients.createDefault();
    client.start();
    if(Utils.isNullOrEmpty(routerUrls)) {
      throw new VeniceException("Router Urls: "+routerUrls+" is not valid");
    }
    this.routerUrls = routerUrls;
    this.localHostname = Utils.getHostName();
    if (logger.isDebugEnabled()) {
      logger.debug("Parsed hostname as: " + localHostname);
    }
    refreshControllerUrl();
  }

  private void refreshControllerUrl(){
    String controllerUrl = controllerUrlFromRouter(routerUrls);
    if (controllerUrl.endsWith("/")){
      this.controllerUrl = controllerUrl.substring(0, controllerUrl.length()-1);
    } else {
      this.controllerUrl = controllerUrl;
    }
    logger.debug("Identified controller URL: " + this.controllerUrl + " from router: " + routerUrls);
  }

  /**
   * If close is not called, a thread is leaked
   */
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      String msg = "Error closing the controller client for " + controllerUrl;
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  private String controllerUrlFromRouter(String routerUrls){
    List<String> routerUrlList = Arrays.asList(routerUrls.split(","));
    Collections.shuffle(routerUrlList);
    Throwable lastException = null;
    for (String routerUrl : routerUrlList) {
      try {
        final HttpGet get = new HttpGet(routerUrl + "/controller");
        HttpResponse response = client.execute(get, null).get();
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
          String responseBody;
          try (InputStream bodyStream = response.getEntity().getContent()) {
            responseBody = IOUtils.toString(bodyStream);
          }
          return responseBody;
        } else {
          throw new VeniceException("Router: " + routerUrl + " returns status code: " + response.getStatusLine().getStatusCode());
        }
      } catch (Exception e) {
        logger.warn("Failed to get controller URL from router: " + routerUrl, e);
        lastException = e;
      }
    }
    throw new VeniceException("Could not get controller url from any router: " + routerUrls, lastException);
  }

  @Deprecated
  private VersionCreationResponse createNewStoreVersion(String clusterName, String storeName, String owner, long storeSize,
                                                      String keySchema, String valueSchema)
      throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> params = newParams();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner));
    params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(storeSize)));
    params.add(new BasicNameValuePair(ControllerApiConstants.KEY_SCHEMA, keySchema));
    params.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchema));
    String responseJson = postRequest(ControllerRoute.CREATE.getPath(), params);
    return mapper.readValue(responseJson, VersionCreationResponse.class);
  }

  /** Only used for tests */
  @Deprecated
  public static VersionCreationResponse createStoreVersion(
      String routerUrl, String clusterName, String storeName, String owner, long storeSize, String keySchema, String valueSchema) {
    try (ControllerClient client = new ControllerClient(routerUrl)){
      return client.createNewStoreVersion(clusterName, storeName, owner, storeSize, keySchema, valueSchema);
    } catch (Exception e){
      return handleError(
          new VeniceException("Error creating version for store: " + storeName, e), new VersionCreationResponse());
    }
  }

  private NewStoreResponse createNewStore(String clusterName, String storeName, String owner)
      throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> params = newParams();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner));
    String responseJson = postRequest(ControllerRoute.NEWSTORE.getPath(), params);
    return mapper.readValue(responseJson, NewStoreResponse.class);
  }

  public static NewStoreResponse createNewStore(String routerUrls, String clusterName, String storeName, String owner){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.createNewStore(clusterName, storeName, owner);
    } catch (Exception e){
      return handleError(new VeniceException("Error creating store: " + storeName, e), new NewStoreResponse());
    }
  }

  private VersionResponse overrideSetActiveVersion(String clusterName, String storeName, int version)
      throws InterruptedException, IOException, ExecutionException {
    List<NameValuePair> params = newParams();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
    String responseJson = postRequest(ControllerRoute.SETVERSION.getPath(), params);
    return mapper.readValue(responseJson, VersionResponse.class);
  }

  public static VersionResponse overrideSetActiveVersion(String routerUrls, String clusterName, String storeName, int version){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.overrideSetActiveVersion(clusterName, storeName, version);
    } catch(Exception e){
      return handleError(new VeniceException("Error setting version.  Storename: " + storeName + " Version: " + version), new VersionResponse());
    }
  }

  private JobStatusQueryResponse queryJobStatus(String clusterName, String kafkaTopic)
      throws ExecutionException, InterruptedException, IOException {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
    String responseJson = getRequest(ControllerRoute.JOB.getPath(), queryParams);
    return mapper.readValue(responseJson, JobStatusQueryResponse.class);
  }

  public static JobStatusQueryResponse queryJobStatus(String routerUrls, String clusterName, String kafkaTopic){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.queryJobStatus(clusterName, kafkaTopic);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying job status for topic: " + kafkaTopic, e), new JobStatusQueryResponse());
    }
  }

  public static JobStatusQueryResponse queryJobStatusWithRetry(String routerUrls, String clusterName, String kafkaTopic, int attempts){
    if (attempts < 1){
      throw new VeniceException("Querying with retries requires at least one attempt, called with " + attempts + " attempts");
    }
    int attemptsRemaining = attempts;
    JobStatusQueryResponse response = JobStatusQueryResponse.createErrorResponse("Request was not attempted");
    while (attemptsRemaining > 0){
      response = queryJobStatus(routerUrls, clusterName, kafkaTopic); /* should allways return a valid object */
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
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    String responseJson = getRequest(ControllerRoute.LIST_STORES.getPath(), queryParams);
    return mapper.readValue(responseJson, MultiStoreResponse.class);
  }

  public static MultiStoreResponse queryStoreList(String routerUrls, String clusterName){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.queryStoreList(clusterName);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying store list for cluster: " + clusterName, e), new MultiStoreResponse());
    }
  }

  private VersionResponse queryNextVersion(String clusterName, String storeName)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String responseJson = getRequest(ControllerRoute.NEXTVERSION.getPath(), queryParams);
    return mapper.readValue(responseJson, VersionResponse.class);
  }

  /**
   * Query the controller for the next version number that can be created.  This number and larger numbers are available
   * Before creating a kafka topic using this version number, be sure to reserve it using the #reserveVersion method
   *
   * @param routerUrls
   * @param clusterName
   * @param storeName
   * @return A VersionResponse object.  the .getVersion() method returns the next available version.
   */
  public static VersionResponse queryNextVersion(String routerUrls, String clusterName, String storeName){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.queryNextVersion(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying next version for store: " + storeName, e), new VersionResponse());
    }
  }


  private VersionResponse queryCurrentVersion(String clusterName, String storeName)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String responseJson = getRequest(ControllerRoute.CURRENT_VERSION.getPath(), queryParams);
    return mapper.readValue(responseJson, VersionResponse.class);
  }

  public static VersionResponse queryCurrentVersion(String routerUrls, String clusterName, String storeName){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.queryCurrentVersion(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying current version for store: " + storeName, e), new VersionResponse());
    }
  }

  private MultiVersionResponse queryActiveVersions(String clusterName, String storeName)
      throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String responseJson = getRequest(ControllerRoute.ACTIVE_VERSIONS.getPath(), queryParams);
    return mapper.readValue(responseJson, MultiVersionResponse.class);
  }

  public static MultiVersionResponse queryActiveVersions(String routerUrls, String clusterName, String storeName){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.queryActiveVersions(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error querying active version for store: " + storeName, e), new MultiVersionResponse());
    }
  }

  private VersionResponse reserveVersion(String clusterName, String storeName, int version)
      throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
    String responseJson = postRequest(ControllerRoute.RESERVE_VERSION.getPath(), queryParams);
    return mapper.readValue(responseJson, VersionResponse.class);
  }

  /**
   * Reserves a version number so another process doens't try and create a store version using that number
   *
   * @param routerUrls
   * @param clusterName
   * @param storeName
   * @param version
   * @return VersionResponse object.  If the reservation fails, this object's .isError() method will return true.
   */
  public static VersionResponse reserveVersion(String routerUrls, String clusterName, String storeName, int version){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.reserveVersion(clusterName, storeName, version);
    } catch (Exception e){
      return handleError(new VeniceException("Error reserving version " + version + " for store: " + storeName, e), new VersionResponse());
    }
  }

  private MultiNodeResponse listStorageNodes(String clusterName)
      throws InterruptedException, IOException, ExecutionException {
    List<NameValuePair> queryParams = newParams(clusterName);
    String responseJson = getRequest(ControllerRoute.LIST_NODES.getPath(), queryParams);
    return mapper.readValue(responseJson, MultiNodeResponse.class);
  }

  public static MultiNodeResponse listStorageNodes(String routerUrls, String clusterName){
    try (ControllerClient client = new ControllerClient(routerUrls)){
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

  public static MultiReplicaResponse listReplicas(String routerUrls, String clusterName, String storeName, int version){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.listReplicas(clusterName, storeName, version);
    } catch (Exception e){
      return handleError(new VeniceException("Error listing replicas for store: " + storeName + " version: " + version, e), new MultiReplicaResponse());
    }
  }

  /* SCHEMA */

  private SchemaResponse initKeySchema(String clusterName, String storeName, String keySchemaStr) throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.KEY_SCHEMA, keySchemaStr));
    String responseJson = postRequest(ControllerRoute.INIT_KEY_SCHEMA.getPath(), queryParams);
    return mapper.readValue(responseJson, SchemaResponse.class);
  }

  public static SchemaResponse initKeySchema(String routerUrls, String clusterName, String storeName, String keySchemaStr) {
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.initKeySchema(clusterName, storeName, keySchemaStr);
    } catch (Exception e){
      return handleError(new VeniceException("Error creating key schema: " + keySchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  private SchemaResponse getKeySchema(String clusterName, String storeName) throws ExecutionException, InterruptedException, IOException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String responseJson = getRequest(ControllerRoute.GET_KEY_SCHEMA.getPath(), queryParams);
    return mapper.readValue(responseJson, SchemaResponse.class);
  }

  public static SchemaResponse getKeySchema(String routerUrls, String clusterName, String storeName) {
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.getKeySchema(clusterName, storeName);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting key schema for store: " + storeName, e), new SchemaResponse());
    }
  }

  private SchemaResponse addValueSchema(String clusterName, String storeName, String valueSchemaStr) throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchemaStr));
    String responseJson = postRequest(ControllerRoute.ADD_VALUE_SCHEMA.getPath(), queryParams);
    return mapper.readValue(responseJson, SchemaResponse.class);
  }

  public static SchemaResponse addValueSchema(String routerUrls, String clusterName, String storeName, String valueSchemaStr) {
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.addValueSchema(clusterName, storeName, valueSchemaStr);
    } catch (Exception e){
      return handleError(new VeniceException("Error adding value schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  private SchemaResponse getValueSchema(String clusterName, String storeName, int valueSchemaId) throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.SCHEMA_ID, Integer.toString(valueSchemaId)));
    String responseJson = getRequest(ControllerRoute.GET_VALUE_SCHEMA.getPath(), queryParams);
    return mapper.readValue(responseJson, SchemaResponse.class);
  }

  public static SchemaResponse getValueSchema(String routerUrls, String clusterName, String storeName, int valueSchemaId) {
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.getValueSchema(clusterName, storeName, valueSchemaId);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema id: " + valueSchemaId + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  private SchemaResponse getValueSchemaID(String clusterName, String storeName, String valueSchemaStr) throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchemaStr));
    String responseJson = postRequest(ControllerRoute.GET_VALUE_SCHEMA_ID.getPath(), queryParams);
    return mapper.readValue(responseJson, SchemaResponse.class);
  }

  public static SchemaResponse getValueSchemaID(String routerUrls, String clusterName, String storeName, String valueSchemaStr) {
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.getValueSchemaID(clusterName, storeName, valueSchemaStr);
    } catch (Exception e){
      return handleError(new VeniceException("Error getting value schema for schema: " + valueSchemaStr + " for store: " + storeName, e), new SchemaResponse());
    }
  }

  private MultiSchemaResponse getAllValueSchema(String clusterName, String storeName) throws IOException, ExecutionException, InterruptedException {
    List<NameValuePair> queryParams = newParams();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
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
    String queryString = URLEncodedUtils.format(params, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(controllerUrl + path + "?" + queryString);
    HttpResponse response = client.execute(get, null).get();
    return getJsonFromHttpResponse(response);
  }

  private String postRequest(String path, List<NameValuePair> params)
      throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    final HttpPost post = new HttpPost(controllerUrl + path);
    post.setEntity(new UrlEncodedFormEntity(params));
    HttpResponse response = client.execute(post, null).get();
    return getJsonFromHttpResponse(response);
  }

  public static MultiSchemaResponse getAllValueSchema(String routerUrls, String clusterName, String storeName) {
    try (ControllerClient client = new ControllerClient(routerUrls)){
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
