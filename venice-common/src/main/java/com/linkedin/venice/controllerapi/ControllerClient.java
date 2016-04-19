package com.linkedin.venice.controllerapi;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.codehaus.jackson.type.TypeReference;


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

  private final static ObjectMapper mapper = new ObjectMapper();
  private final static Logger logger = Logger.getLogger(ControllerClient.class.getName());

  /**
   * It creates a thread for sending Http Requests.
   *
   * @param controllerUrl url of the Venice Controller.
   */
  private ControllerClient(String controllerUrl){
    client = HttpAsyncClients.createDefault();
    client.start();
    if(Utils.isNullOrEmpty(controllerUrl)) {
      throw new VeniceException("Controller Url is not valid");
    }
    if (controllerUrl.endsWith("/")){
      this.controllerUrl = controllerUrl.substring(0, controllerUrl.length()-1);
    } else {
      this.controllerUrl = controllerUrl;
    }
    logger.debug("Created client with URL: " + this.controllerUrl + " from input: " + controllerUrl);
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

  private StoreCreationResponse createNewStoreVersion(String clusterName, String storeName, String owner, long storeSize,
                                                      String keySchema, String valueSchema){
    try {
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
      String responseBody;
      try (InputStream bodyStream = response.getEntity().getContent()) {
        responseBody = IOUtils.toString(bodyStream);
      }
      String contentType = response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue();
      if (contentType.startsWith(HttpConstants.JSON)) {
        return mapper.readValue(responseBody, StoreCreationResponse.class);
      } else { //non JSON response
        String msg = "For store: " + storeName + ", controller returns with content-type " + contentType + ": " + responseBody;
        logger.error(msg);
        throw new VeniceException(msg);
      }
    } catch (Exception e) {
      String msg = "Error creating Store: " + storeName + " Owner: " + owner + " Size: " + storeSize + " bytes";
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  public static StoreCreationResponse createStoreVersion(String controllerUrl, String clusterName, String storeName, String owner,
                                                         long storeSize, String keySchema, String valueSchema) {
    try (ControllerClient client = new ControllerClient(controllerUrl)){
        return client.createNewStoreVersion(clusterName, storeName, owner, storeSize, keySchema, valueSchema);
    }
  }

  private void overrideSetActiveVersion(String clusterName, String storeName, int version){
    try {
      final HttpPost post = new HttpPost(controllerUrl + ControllerApiConstants.SETVERSION_PATH);
      List<NameValuePair> params = new ArrayList<NameValuePair>();
      params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      params.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
      post.setEntity(new UrlEncodedFormEntity(params));
      HttpResponse response = client.execute(post, null).get();
    } catch(Exception e){
      String msg = "Error setting version.  Storename: " + storeName + " Version: " + version;
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  public static void overrideSetActiveVersion(String controllerUrl, String clusterName, String storeName, int version){
    try (ControllerClient client = new ControllerClient(controllerUrl)){
      client.overrideSetActiveVersion(clusterName, storeName, version);
    }
  }

  private JobStatusQueryResponse queryJobStatus(String clusterName, String kafkaTopic){
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    try{
      List<NameValuePair> queryParams = new ArrayList<>();
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      queryParams.add(new BasicNameValuePair(ControllerApiConstants.VERSION, Integer.toString(version)));
      String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);
      final HttpGet get = new HttpGet(controllerUrl + ControllerApiConstants.JOB_PATH + "?" + queryString);
      HttpResponse response = client.execute(get, null).get();
      String responseBody;
      try (InputStream bodyStream = response.getEntity().getContent()) {
        responseBody = IOUtils.toString(bodyStream);
      }
      String contentType = response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue();
      if (contentType.startsWith(HttpConstants.JSON)) {
        return mapper.readValue(responseBody, JobStatusQueryResponse.class);
      } else { //non JSON response
        String msg = "For store: " + storeName + ", controller returns with content-type " + contentType + ": " + responseBody;
        logger.error(msg);
        throw new VeniceException(msg);
      }
    } catch (Exception e) {
      String msg = "Error creating Store: " + storeName + " Version: " + version;
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  public static JobStatusQueryResponse queryJobStatus(String controllerUrl, String clusterName, String kafkaTopic){
    try (ControllerClient client = new ControllerClient(controllerUrl)){
      return client.queryJobStatus(clusterName, kafkaTopic);
    }
  }

  /**
   *
   * @param startSleep initial amount of time to wait between successive polls
   * @param maxSleep max amount of time to wait between successive polls
   * @param veniceControllerUrl URL for connecting to Venice Controller
   * @param topic kafka topic for the store we want to query
   */
  public static void pollJobStatusUntilFinished(long startSleep, long maxSleep, String veniceControllerUrl, String clusterName, String topic) {
    JobStatusQueryResponse queryResponse = null;
    String status = null;
    long sleepTime = startSleep;
    boolean jobDone = false;
    try (ControllerClient client = new ControllerClient(veniceControllerUrl)) {
      while (!jobDone) {

        int retry = 2; /* if the query returns an error object, retry a couple times before failing */
        for(;;) { //TODO: switch to exponential backoff retry policy
          queryResponse = client.queryJobStatus(clusterName, topic);
          if (queryResponse.getError() != null) {
            if (retry <= 0) {
              throw new VeniceException("Error getting status of push: " + queryResponse.getError());
            } else {
              logger.warn("Error querying push status: " + queryResponse.getError() + " Retrying...");
              Thread.sleep(1000);
              retry--;
            }
          } else {
            break;
          }
        }

        status = queryResponse.getStatus();
        if (status.equals(ExecutionStatus.NEW.toString())
            || status.equals(ExecutionStatus.STARTED.toString())
            || status.equals(ExecutionStatus.PROGRESS.toString())) {
          logger.info("Push status: " + status + "...");
          Thread.sleep(sleepTime);
          sleepTime = sleepTime * 2;
          if (sleepTime > maxSleep) {
            sleepTime = maxSleep;
          }
        } else {
          jobDone = true;
        }
      }  //end while

    /* If we get here, status exists and is not null (error threw an exception) */
      if (status.equals(ExecutionStatus.COMPLETED.toString())) {
        logger.info("Push job completed successfully");
      } else {
        throw new VeniceException("Push job resulted in a status of: " + status);
      }
    } catch (InterruptedException e) {
      logger.error(e);
      throw new VeniceException("Polling of push status was interrupted");
    }
  }
}
