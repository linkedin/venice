package com.linkedin.venice.controllerapi;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
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
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Controller Client to talk to Venice Controller.
 * If close method is not called at the end of the usage, it leaks
 * a thread and the Process never shuts down. If it is required
 * for just making a single call use the static utility method
 * to avoid the thread leak.
 */
public class ControllerClient implements Closeable {
  private final CloseableHttpAsyncClient client;
  private String controllerUrl;
  private String routerUrls;

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

  private VersionCreationResponse createNewStoreVersion(String clusterName, String storeName, String owner, long storeSize,
                                                      String keySchema, String valueSchema)
      throws IOException, ExecutionException, InterruptedException {
    final HttpPost post = new HttpPost(controllerUrl + ControllerApiConstants.CREATE_PATH);
    List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner));
    params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(storeSize)));
    params.add(new BasicNameValuePair(ControllerApiConstants.KEY_SCHEMA, keySchema));
    params.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchema));
    post.setEntity(new UrlEncodedFormEntity(params));
    HttpResponse response = client.execute(post, null).get();
    String responseJson = getJsonFromHttpResponse(response);
    return mapper.readValue(responseJson, VersionCreationResponse.class);
  }

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
    final HttpPost post = new HttpPost(controllerUrl + ControllerApiConstants.NEWSTORE_PATH);
    List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner));
    post.setEntity(new UrlEncodedFormEntity(params));
    HttpResponse response = client.execute(post, null).get();
    String responseJson = getJsonFromHttpResponse(response);
    return mapper.readValue(responseJson, NewStoreResponse.class);
  }

  public static NewStoreResponse createNewStore(String routerUrls, String clusterName, String storeName, String owner){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      return client.createNewStore(clusterName, storeName, owner);
    } catch (Exception e){
      return handleError(new VeniceException("Error creating store: " + storeName, e), new NewStoreResponse());
    }
  }

  private void overrideSetActiveVersion(String clusterName, String storeName, int version)
      throws UnsupportedEncodingException, ExecutionException, InterruptedException {
    final HttpPost post = new HttpPost(controllerUrl + ControllerApiConstants.SETVERSION_PATH);
    List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
    post.setEntity(new UrlEncodedFormEntity(params));
    client.execute(post, null).get();
  }

  public static void overrideSetActiveVersion(String routerUrls, String clusterName, String storeName, int version){
    try (ControllerClient client = new ControllerClient(routerUrls)){
      client.overrideSetActiveVersion(clusterName, storeName, version);
    } catch(Exception e){
      String msg = "Error setting version.  Storename: " + storeName + " Version: " + version;
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  private JobStatusQueryResponse queryJobStatus(String clusterName, String kafkaTopic)
      throws ExecutionException, InterruptedException, IOException {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(controllerUrl + ControllerApiConstants.JOB_PATH + "?" + queryString);
    HttpResponse response = client.execute(get, null).get();
    String responseJson = getJsonFromHttpResponse(response);
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
    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(controllerUrl + ControllerApiConstants.LIST_STORES_PATH + "?" + queryString);
    HttpResponse response = client.execute(get, null).get();
    String responseJson = getJsonFromHttpResponse(response);
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
    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(controllerUrl + ControllerApiConstants.NEXTVERSION_PATH + "?" + queryString);
    HttpResponse response = client.execute(get, null).get();
    String responseJson = getJsonFromHttpResponse(response);
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
    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(controllerUrl + ControllerApiConstants.CURRENT_VERSION_PATH + "?" + queryString);
    HttpResponse response = client.execute(get, null).get();
    String responseJson = getJsonFromHttpResponse(response);
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
    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(controllerUrl + ControllerApiConstants.ACTIVE_VERSIONS_PATH + "?" + queryString);
    HttpResponse response = client.execute(get, null).get();
    String responseJson = getJsonFromHttpResponse(response);
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
    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
    final HttpPost post = new HttpPost(controllerUrl + ControllerApiConstants.RESERVE_VERSION_PATH);
    post.setEntity(new UrlEncodedFormEntity(queryParams));
    HttpResponse response = client.execute(post, null).get();
    String responseJson = getJsonFromHttpResponse(response);
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
