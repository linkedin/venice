package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;


public class HttpsTransportClient extends HttpTransportClient {
  private boolean requireHTTP2;
  private SSLFactory sslFactory;

  public HttpsTransportClient(
      String routerUrl,
      int maxConnectionsTotal,
      int maxConnectionsPerRoute,
      boolean requireHTTP2,
      SSLFactory sslFactory) {
    this(routerUrl, buildClient(routerUrl, maxConnectionsTotal, maxConnectionsPerRoute, requireHTTP2, sslFactory));
    this.maxConnectionsTotal = maxConnectionsTotal;
    this.maxConnectionsPerRoute = maxConnectionsPerRoute;
    this.requireHTTP2 = requireHTTP2;
    this.sslFactory = sslFactory;
  }

  public HttpsTransportClient(String routerUrl, CloseableHttpAsyncClient client) {
    super(routerUrl, client);
    if (!routerUrl.startsWith(HTTPS)) {
      throw new VeniceException("Must use https url with HttpsTransportClient, found: " + routerUrl);
    }
  }

  public boolean isRequireHTTP2() {
    return requireHTTP2;
  }

  /**
   * The same {@link CloseableHttpAsyncClient} could not be used to send out another request in its own callback function.
   * @return
   */
  @Override
  public TransportClient getCopyIfNotUsableInCallback() {
    return new HttpsTransportClient(routerUrl, maxConnectionsTotal, maxConnectionsPerRoute, requireHTTP2, sslFactory);
  }
}
