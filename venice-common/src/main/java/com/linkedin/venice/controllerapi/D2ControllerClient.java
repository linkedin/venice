package com.linkedin.venice.controllerapi;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import org.apache.http.client.utils.URLEncodedUtils;

import java.nio.charset.StandardCharsets;

public class D2ControllerClient extends ControllerClient{
  private final static String D2_PREFIX = "d2://";
  private final static String D2_SERVICE_NAME = "VeniceRouter";

  private D2Client d2Client;

  public D2ControllerClient(String clusterName, String urlsToFindMasterController) {
    super(clusterName, urlsToFindMasterController);
    d2Client = new D2ClientBuilder().setZkHosts(urlsToFindMasterController).build();
    D2ClientUtils.startClient(d2Client);
  }

  @Override
  protected String getMasterControllerUrl(String urlsToFindMasterController) {
    String path = ControllerRoute.MASTER_CONTROLLER.getPath();
    String params = URLEncodedUtils.format(newParams(getClusterName()), StandardCharsets.UTF_8);
    String requestPath = D2_PREFIX + D2_SERVICE_NAME + path + "?" + params;

    try {
      RestResponse response = D2ClientUtils.sendD2GetRequest(requestPath, d2Client);
      String responseBody = response.getEntity().asString(StandardCharsets.UTF_8);

      MasterControllerResponse controllerResponse = mapper.readValue(responseBody, MasterControllerResponse.class);
      if (controllerResponse.isError()) {
        throw new VeniceException("Received error response: [" + mapper.writeValueAsString(controllerResponse) + "]");
      }

      return controllerResponse.getUrl();
    } catch (Exception e) {
      throw new VeniceException("Failed to get master controller uri from zk, url: " + urlsToFindMasterController, e);
    }
  }

  @Override
  public void close() {
    super.close();
    D2ClientUtils.shutdownClient(d2Client);
  }
}
