package com.linkedin.venice.controllerapi;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;


public class D2ControllerClient extends ControllerClient{
  private final static String D2_SCHEME = "d2://";
  /**
   * {@link #DUMMY_URL_WHEN_USING_D2_CLIENT} is not used since {@link D2ControllerClient}
   * will use D2Client to fetch the controller URL.
   */
  private final static String DUMMY_URL_WHEN_USING_D2_CLIENT = "http://fake.host";

  private final String d2ServiceName;
  private final D2Client d2Client;
  private final boolean externalD2Client;
  private final Optional<SSLFactory> sslFactory;

  public D2ControllerClient(String d2ServiceName, String clusterName, String d2ZKHost, Optional<SSLFactory> sslFactory) {
    super(clusterName, DUMMY_URL_WHEN_USING_D2_CLIENT, sslFactory);
    this.d2ServiceName = d2ServiceName;
    this.d2Client = new D2ClientBuilder()
                   .setZkHosts(d2ZKHost)
                   .build();
    D2ClientUtils.startClient(d2Client);
    this.externalD2Client = false;
    this.sslFactory = sslFactory;
  }

  public D2ControllerClient(String d2ServiceName, String clusterName, D2Client d2Client) {
    this(d2ServiceName, clusterName, d2Client, Optional.empty());
  }

  public D2ControllerClient(String d2ServiceName, String clusterName, D2Client d2Client, Optional<SSLFactory> sslFactory) {
    super(clusterName, DUMMY_URL_WHEN_USING_D2_CLIENT, sslFactory);
    this.d2ServiceName = d2ServiceName;
    this.d2Client = d2Client;
    this.externalD2Client = true;
    this.sslFactory = sslFactory;
  }

  @Override
  protected String discoverLeaderController() {
    LeaderControllerResponse controllerResponse = d2ClientGet(
        this.d2Client,
        this.d2ServiceName,
        // TODO: Change this to LEADER_CONTROLLER after backend components with inclusive endpoints are deployed completely
        ControllerRoute.MASTER_CONTROLLER.getPath(),
        newParams(),
        LeaderControllerResponse.class);
    /**
     * Current controller D2 announcement is announcing url with http: prefix and the regular HTTP port number (1576);
     * if we change the D2 announcement, the existing Samza users which depend on D2 result would break; if we upgrade
     * Samza lib first, they would get the original D2 result (1576) which is wrong. To ensure none of these happen,
     * we need a synchronized deployment for updating D2 announcement and all samza users at the same time.. which is
     * impossible.
     *
     * The below logic is a workaround to allow client to recognize both the old D2 result and new D2 result, which
     * would achieve a smooth migration. Notice that all Samza customers must be upgraded first before changing D2 announcement.
     *
     * TODO: Remove the code below after controller ACL migration is completed.
     */
    if (sslFactory.isPresent()) {
      try {
        URL responseUrl = new URL(controllerResponse.getUrl());
        if (responseUrl.getProtocol().equalsIgnoreCase("http")) {
          URL secureControllerUrl = convertToSecureUrl(responseUrl, 1578);
          return secureControllerUrl.toString();
        }
      } catch (MalformedURLException e) {
        throw new VeniceException("Error when building URL.", e);
      }
    }
    return controllerResponse.getUrl();
  }

  private static <RESPONSE> RESPONSE d2ClientGet(D2Client d2Client, String d2ServiceName, String path,
      QueryParams params, Class<RESPONSE> responseClass) {
    String requestPath = D2_SCHEME + d2ServiceName + path + "?" + encodeQueryParams(params);
    try {
      RestResponse response = D2ClientUtils.sendD2GetRequest(requestPath, d2Client);
      String responseBody = response.getEntity().asString(StandardCharsets.UTF_8);
      return ControllerTransport.getObjectMapper().readValue(responseBody, responseClass);
    } catch (Exception e) {
      throw new VeniceException("Failed to get response for url: " + requestPath + " with D2 Client", e);
    }
  }

  public static D2ServiceDiscoveryResponse discoverCluster(D2Client d2Client, String d2ServiceName, String storeName) {
    return d2ClientGet(d2Client, d2ServiceName, ControllerRoute.CLUSTER_DISCOVERY.getPath(),
        getQueryParamsToDiscoverCluster(storeName), D2ServiceDiscoveryResponse.class);
  }

  /**
   * Here, if discovery fails, we will throw a VeniceException.
   */

  public static D2ServiceDiscoveryResponse discoverCluster(String d2ZkHost, String d2ServiceName, String storeName) throws VeniceException {
    D2Client d2Client = new D2ClientBuilder()
        .setZkHosts(d2ZkHost)
        .build();
    try {
      D2ClientUtils.startClient(d2Client);
      D2ServiceDiscoveryResponse discoResponse = discoverCluster(d2Client, d2ServiceName, storeName);
      if (discoResponse.isError())
        throw new VeniceException(discoResponse.getError());
      return discoResponse;
    }
    finally {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  public static D2ControllerClient discoverAndConstructControllerClient(String storeName, String d2ServiceName, String d2ZkHost) {
    return D2ControllerClient.constructClusterControllerClient(storeName, d2ServiceName, d2ZkHost, Optional.empty());
  }

  public static D2ControllerClient discoverAndConstructControllerClient(String storeName, String d2ServiceName, String d2ZkHost, Optional<SSLFactory> sslFactory) {
    D2ServiceDiscoveryResponse discoResponse = discoverCluster(d2ZkHost, d2ServiceName, storeName);
    if (!clusterToClientMapContains(discoResponse.getCluster(), d2ServiceName))
      addClusterToClientMapEntry(discoResponse.getCluster(), d2ServiceName, new D2ControllerClient(d2ServiceName, discoResponse.getCluster(), d2ZkHost, sslFactory));
    return constructClusterControllerClient(discoResponse.getCluster(), d2ServiceName, d2ZkHost, sslFactory);
  }

  public static D2ControllerClient constructClusterControllerClient(String clusterName, String d2ServiceName, String d2ZkHost) {
    return D2ControllerClient.constructClusterControllerClient(clusterName, d2ServiceName, d2ZkHost, Optional.empty());
  }

  public static D2ControllerClient constructClusterControllerClient(String clusterName, String d2ServiceName, String d2ZkHost, Optional<SSLFactory> sslFactory) {
    if (!clusterToClientMapContains(clusterName, d2ServiceName))
      addClusterToClientMapEntry(clusterName, d2ServiceName, new D2ControllerClient(d2ServiceName, clusterName, d2ZkHost, sslFactory));
    return (D2ControllerClient) getClusterToClientMapEntry(clusterName, d2ServiceName);
  }

  @Override
  public void close() {
    super.close();
    if (!this.externalD2Client) {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  /**
   * Convert a HTTP url to HTTPS url with specific port number;
   * TODO: remove the below helper function after Controller ACL migration.
   */
  public static URL convertToSecureUrl(URL url, int port) throws MalformedURLException {
    return new URL("https", url.getHost(), port, url.getFile());
  }
}
