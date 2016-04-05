package com.linkedin.venice.controllerapi;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
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
    logger.info("Created client with URL: " + this.controllerUrl + " from input: " + controllerUrl);
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

  private StoreCreationResponse createNewStoreVersion(String storeName, String owner, int storeSizeMb){
    try {
      final HttpPost post = new HttpPost(controllerUrl + ControllerApiConstants.CREATE_PATH);
      List<NameValuePair> params = new ArrayList<NameValuePair>();
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
      params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, owner));
      params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Integer.toString(storeSizeMb)));
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
      String msg = "Error creating Store: " + storeName + " Owner: " + owner + " Size: " + storeSizeMb;
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  public static StoreCreationResponse createStoreVersion(String controllerUrl,String storeName, String owner, int storeSizeMb ) {
    try (ControllerClient client = new ControllerClient(controllerUrl)){
        return client.createNewStoreVersion(storeName, owner, storeSizeMb);
    }
  }

  private JobStatusQueryResponse queryJobStatus(String storeName, int version){
    try{
      List<NameValuePair> queryParams = new ArrayList<>();
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

  public static JobStatusQueryResponse queryJobStatus(String controllerUrl,String storeName, int version) {
    try (ControllerClient client = new ControllerClient(controllerUrl)){
      return client.queryJobStatus(storeName, version);
    }
  }
}
