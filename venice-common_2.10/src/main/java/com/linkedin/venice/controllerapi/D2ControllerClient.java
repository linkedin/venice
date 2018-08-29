package com.linkedin.venice.controllerapi;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.exceptions.VeniceException;

import java.nio.charset.StandardCharsets;


public class D2ControllerClient extends ControllerClient{
  private final static String D2_SCHEME = "d2://";
  private final static String DUMMY_URL_WHEN_USING_D2_CLIENT = "http://fake.host";

  private final String d2ServiceName;
  private final D2Client d2Client;
  private final boolean externalD2Client;


  public D2ControllerClient(String d2ServiceName, String clusterName, String d2ZKHost) {
    /**
     * {@link #DUMMY_URL_WHEN_USING_D2_CLIENT} is not used since {@link D2ControllerClient}
     * will use D2Client to fetch the controller URL.
     */
    super(clusterName, DUMMY_URL_WHEN_USING_D2_CLIENT);
    this.d2ServiceName = d2ServiceName;
    this.d2Client = new D2ClientBuilder()
                   .setZkHosts(d2ZKHost)
                   .build();
    D2ClientUtils.startClient(d2Client);
    this.externalD2Client = false;
  }

  public D2ControllerClient(String d2ServiceName, String clusterName, D2Client d2Client) {
    super(clusterName, DUMMY_URL_WHEN_USING_D2_CLIENT);
    this.d2ServiceName = d2ServiceName;
    this.d2Client = d2Client;
    this.externalD2Client = true;
  }


  @Override
  protected String getMasterControllerUrl(String urlsToFindMasterController) {
    MasterControllerResponse controllerResponse = d2ClientGet(d2Client, d2ServiceName,
        ControllerRoute.MASTER_CONTROLLER.getPath(),
        newParams(getClusterName()),
        MasterControllerResponse.class);
    return controllerResponse.getUrl();
  }

  private static <RESPONSE> RESPONSE d2ClientGet(D2Client d2Client, String d2ServiceName, String path,
      QueryParams params, Class<RESPONSE> responseClass) {
    String requestPath = D2_SCHEME + d2ServiceName + path + "?" + encodeQueryParams(params);
    try {
      RestResponse response = D2ClientUtils.sendD2GetRequest(requestPath, d2Client);
      String responseBody = response.getEntity().asString(StandardCharsets.UTF_8);
      return mapper.readValue(responseBody, responseClass);
    } catch (Exception e) {
      throw new VeniceException("Failed to get response for url: " + requestPath + " with D2 Client", e);
    }
  }


  public static D2ServiceDiscoveryResponse discoverCluster(D2Client d2Client, String d2ServiceName, String storeName) {
    return d2ClientGet(d2Client, d2ServiceName, ControllerRoute.CLUSTER_DISCOVERY.getPath(),
        getQueryParamsToDiscoverCluster(storeName), D2ServiceDiscoveryResponse.class);
  }

  @Override
  public void close() {
    super.close();
    if (!this.externalD2Client) {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }
}
