package com.linkedin.venice.controllerapi;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.exceptions.VeniceException;
import org.apache.http.client.utils.URLEncodedUtils;

import java.nio.charset.StandardCharsets;

public class D2ControllerClient extends ControllerClient{
  private final static String D2_SCHEME = "d2://";

  private String d2ServiceName;
  private D2Client d2Client;

  public D2ControllerClient(String d2ServiceName, String clusterName, String d2ZKHost) {
    super(clusterName, d2ZKHost);
    this.d2ServiceName = d2ServiceName;
    d2Client = new D2ClientBuilder()
                   .setZkHosts(d2ZKHost)
                   .build();
    D2ClientUtils.startClient(d2Client);
  }

  @Override
  protected String getMasterControllerUrl(String urlsToFindMasterController) {
    String path = ControllerRoute.MASTER_CONTROLLER.getPath();
    String params = URLEncodedUtils.format(newParams(getClusterName()).getNameValuePairs(), StandardCharsets.UTF_8);
    String requestPath = D2_SCHEME + d2ServiceName + path + "?" + params;

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
