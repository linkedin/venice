package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.authentication.ClientAuthenticationProvider;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;


public class HttpsTransportClient extends HttpTransportClient {
  private SSLFactory sslFactory;

  public HttpsTransportClient(
      String routerUrl,
      SSLFactory sslFactory,
      ClientAuthenticationProvider authenticationProvider) {
    this(
        routerUrl,
        HttpAsyncClients.custom().setSSLStrategy(new SSLIOSessionStrategy(sslFactory.getSSLContext())).build(),
        authenticationProvider);
    this.sslFactory = sslFactory;
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

  /**
   * The same {@link CloseableHttpAsyncClient} could not be used to send out another request in its own callback function.
   * @return
   */
  @Override
  public TransportClient getCopyIfNotUsableInCallback() {
    return new HttpsTransportClient(routerUrl, sslFactory, authenticationProvider);
  }
}
