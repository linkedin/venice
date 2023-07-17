package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.authentication.ClientAuthenticationProvider;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;


public class HttpsTransportClient extends HttpTransportClient {
  private boolean requireHTTP2;

  public HttpsTransportClient(
      String routerUrl,
      int maxConnectionsTotal,
      int maxConnectionsPerRoute,
      boolean requireHTTP2,
      SSLFactory sslFactory,
      ClientAuthenticationProvider authenticationProvider) {
    this(
        routerUrl,
        buildClient(routerUrl, maxConnectionsTotal, maxConnectionsPerRoute, requireHTTP2, sslFactory),
        authenticationProvider);
    this.maxConnectionsTotal = maxConnectionsTotal;
    this.maxConnectionsPerRoute = maxConnectionsPerRoute;
    this.requireHTTP2 = requireHTTP2;

  }

  public HttpsTransportClient(
      String routerUrl,
      CloseableHttpAsyncClient client,
      ClientAuthenticationProvider authenticationProvider) {
    super(routerUrl, client, authenticationProvider);
    if (!routerUrl.startsWith(HTTPS)) {
      throw new VeniceException("Must use https url with HttpsTransportClient, found: " + routerUrl);
    }
  }

  public boolean isRequireHTTP2() {
    return requireHTTP2;
  }
}
