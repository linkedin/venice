package com.linkedin.venice.controllerapi;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.d2.D2ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ReferenceCounted;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;


public class D2ControllerClient extends ControllerClient {
  private static final String D2_SCHEME = "d2://";
  /**
   * {@link #DUMMY_URL_WHEN_USING_D2_CLIENT} is not used since {@link D2ControllerClient}
   * will use D2Client to fetch the controller URL.
   */
  private static final String DUMMY_URL_WHEN_USING_D2_CLIENT = "http://fake.host";

  private final String d2ServiceName;
  private final ReferenceCounted<D2Client> d2Client;
  private final boolean externalD2Client;
  private final Optional<SSLFactory> sslFactory;
  private final String controllerClientCacheKey;

  public D2ControllerClient(
      String d2ServiceName,
      String clusterName,
      String d2ZKHost,
      Optional<SSLFactory> sslFactory) {
    super(clusterName, DUMMY_URL_WHEN_USING_D2_CLIENT, sslFactory);
    this.d2ServiceName = d2ServiceName;
    this.d2Client = D2ClientFactory.getD2Client(d2ZKHost, sslFactory);
    this.externalD2Client = false;
    this.sslFactory = sslFactory;
    this.controllerClientCacheKey = getControllerClientCacheKey(clusterName, d2ServiceName, d2ZKHost);
  }

  public D2ControllerClient(String d2ServiceName, String clusterName, D2Client d2Client) {
    this(d2ServiceName, clusterName, d2Client, Optional.empty());
  }

  public D2ControllerClient(
      String d2ServiceName,
      String clusterName,
      D2Client d2Client,
      Optional<SSLFactory> sslFactory) {
    super(clusterName, DUMMY_URL_WHEN_USING_D2_CLIENT, sslFactory);
    this.d2ServiceName = d2ServiceName;
    this.d2Client = new ReferenceCounted<>(d2Client, client -> {});
    this.externalD2Client = true;
    this.sslFactory = sslFactory;
    this.controllerClientCacheKey = getControllerClientCacheKey(clusterName, d2ServiceName, d2Client);
  }

  @Override
  protected String discoverLeaderController() {
    LeaderControllerResponse controllerResponse = d2ClientGet(
        this.d2Client.get(),
        this.d2ServiceName,
        ControllerRoute.LEADER_CONTROLLER.getPath(),
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
        if (controllerResponse.getSecureUrl() != null) {
          return controllerResponse.getSecureUrl();
        }

        URL responseUrl = new URL(controllerResponse.getUrl());
        if (responseUrl.getProtocol().equalsIgnoreCase("http")) {
          URL secureControllerUrl = convertToSecureUrl(responseUrl);
          return secureControllerUrl.toString();
        }
      } catch (MalformedURLException e) {
        throw new VeniceException("Error when building URL.", e);
      }
    }
    return controllerResponse.getUrl();
  }

  private static <RESPONSE> RESPONSE d2ClientGet(
      D2Client d2Client,
      String d2ServiceName,
      String path,
      QueryParams params,
      Class<RESPONSE> responseClass) {
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
    return d2ClientGet(
        d2Client,
        d2ServiceName,
        ControllerRoute.CLUSTER_DISCOVERY.getPath(),
        getQueryParamsToDiscoverCluster(storeName),
        D2ServiceDiscoveryResponse.class);
  }

  public static D2ServiceDiscoveryResponse discoverCluster(String d2ZkHost, String d2ServiceName, String storeName)
      throws VeniceException {
    return discoverCluster(d2ZkHost, d2ServiceName, storeName, 1);
  }

  public static D2ServiceDiscoveryResponse discoverCluster(
      String d2ZkHost,
      String d2ServiceName,
      String storeName,
      int retryAttempts) {
    D2Client d2Client = new D2ClientBuilder().setZkHosts(d2ZkHost).build();
    try {
      D2ClientUtils.startClient(d2Client);
      return discoverCluster(d2Client, d2ServiceName, storeName, retryAttempts);
    } finally {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  /**
   * Here, if discovery fails, we will throw a VeniceException.
   */
  public static D2ServiceDiscoveryResponse discoverCluster(
      D2Client d2Client,
      String d2ServiceName,
      String storeName,
      int retryAttempts) {
    try (D2ControllerClient client = new D2ControllerClient(d2ServiceName, "*", d2Client)) {
      D2ServiceDiscoveryResponse discoResponse =
          retryableRequest(client, retryAttempts, c -> discoverCluster(d2Client, d2ServiceName, storeName));
      if (discoResponse.isError()) {
        throw new VeniceException(discoResponse.getError());
      }
      return discoResponse;
    }
  }

  @Override
  protected String getControllerClientCacheKey() {
    return controllerClientCacheKey;
  }

  private static String getControllerClientCacheKey(String clusterName, String d2ServiceName, String d2ZkHost) {
    return clusterName + d2ServiceName + d2ZkHost;
  }

  private static String getControllerClientCacheKey(String clusterName, String d2ServiceName, D2Client d2Client) {
    return clusterName + d2ServiceName + d2Client.hashCode();
  }

  public static D2ControllerClient discoverAndConstructControllerClient(
      String storeName,
      String d2ServiceName,
      String d2ZkHost,
      Optional<SSLFactory> sslFactory,
      int retryAttempts) {
    D2ServiceDiscoveryResponse discoResponse = discoverCluster(d2ZkHost, d2ServiceName, storeName, retryAttempts);
    if (!clusterToClientMapContains(getControllerClientCacheKey(discoResponse.getCluster(), d2ServiceName, d2ZkHost))) {
      addClusterToClientMapEntry(
          new D2ControllerClient(d2ServiceName, discoResponse.getCluster(), d2ZkHost, sslFactory));
    }
    return constructClusterControllerClient(discoResponse.getCluster(), d2ServiceName, d2ZkHost, sslFactory);
  }

  public static D2ControllerClient constructClusterControllerClient(
      String clusterName,
      String d2ServiceName,
      String d2ZkHost) {
    return D2ControllerClient.constructClusterControllerClient(clusterName, d2ServiceName, d2ZkHost, Optional.empty());
  }

  private static D2ControllerClient constructClusterControllerClient(
      String clusterName,
      String d2ServiceName,
      String d2ZkHost,
      Optional<SSLFactory> sslFactory) {
    String controllerClientCacheKey = getControllerClientCacheKey(clusterName, d2ServiceName, d2ZkHost);
    if (!clusterToClientMapContains(controllerClientCacheKey)) {
      D2ControllerClient controllerClient = new D2ControllerClient(d2ServiceName, clusterName, d2ZkHost, sslFactory);
      addClusterToClientMapEntry(controllerClient);
      return controllerClient;
    } else {
      ReferenceCounted<ControllerClient> controllerClientRef = getClusterToClientMapEntry(controllerClientCacheKey);
      controllerClientRef.retain();
      return (D2ControllerClient) controllerClientRef.get();
    }
  }

  @Override
  public D2ServiceDiscoveryResponse discoverCluster(String storeName) {
    return discoverCluster(d2Client.get(), d2ServiceName, storeName, 1);
  }

  @Override
  public void close() {
    super.close();
    if (this.externalD2Client) {
      d2Client.release();
    }
  }

  /**
   * Convert a HTTP url to HTTPS url with 1578 port number;
   * TODO: remove the below helper functions after Controller ACL migration.
   * @deprecated
   */
  @Deprecated
  public static URL convertToSecureUrl(URL url) throws MalformedURLException {
    return convertToSecureUrl(url, 1578);
  }

  /**
   * Convert a HTTP url to HTTPS url with specific port number;
   * TODO: remove the below helper functions after Controller ACL migration.
   * @deprecated
   */
  @Deprecated
  public static URL convertToSecureUrl(URL url, int port) throws MalformedURLException {
    return new URL("https", url.getHost(), port, url.getFile());
  }
}
