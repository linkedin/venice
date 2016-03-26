package com.linkedin.venice.controllerapi;

import com.linkedin.venice.exceptions.VeniceException;
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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * Created by mwise on 3/17/16.
 */
public class ControllerClient implements Closeable {
  private final CloseableHttpAsyncClient client;
  private String controller;

  private final static ObjectMapper mapper = new ObjectMapper();
  private final static Logger logger = Logger.getLogger(ControllerClient.class.getName());

  public ControllerClient(String controllerHost, int controllerPort){
    client = HttpAsyncClients.createDefault();
    client.start();
    controller = controllerHost + ":" + controllerPort;
  }

  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      String msg = "Error closing the controller client";
      logger.error(msg, e);
      throw new VeniceException(msg, e);
    }
  }

  public StoreCreationResponse CreateNewStoreVersion(String storeName, String owner, int storeSizeMb){
    try {
      final HttpPost post = new HttpPost("http://" + controller + "/create");
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
      if (contentType.startsWith(ControllerApiConstants.JSON)) {
        Map<String, Object> responseMap = fromJson(responseBody);
        return new StoreCreationResponse(storeName, owner, responseMap);
      } else { //non JSON response
        String msg = "For store: " + storeName + ", controller returns with content-type " + contentType + ": " + responseBody;
        logger.error(msg);
        throw new VeniceException(msg);
      }
    } catch (Exception e) {
      logger.error(e);
      throw new VeniceException(e);
    }
  }

  public Map<String, Object> fromJson(String response) {
    try {
      return mapper.readValue(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)),
          new TypeReference<HashMap<String, Object>>() {});
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }
}
