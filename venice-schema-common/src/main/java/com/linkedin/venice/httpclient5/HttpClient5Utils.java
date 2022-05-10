package com.linkedin.venice.httpclient5;

import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.async.MinimalHttpAsyncClient;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.http2.config.H2Config;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;


/**
 * This class is used to provide a facility to initialize httpclient5 lib based Http Client, which supports both http/1.1 and http/2.
 */
public class HttpClient5Utils {
  /**
   * This builder will initialize a httpclient5 based Http Client, which supports both http/1.1 and http/2.
   * For http/1.1, this builder doesn't offer configurable connection management, which means it is less preferred to
   * use http/1.1 with this builder, but the http/1.1 support is more like a fallback.
   */
  public static class HttpClient5Builder {
    private SSLContext sslContext;
    private long requestTimeOutInMilliseconds = TimeUnit.SECONDS.toMillis(1); // 1s by default
    private int ioThreadCount = 48;
    private int maxTotalConnection = 7200;
    private int maxConnectionPerRoute = 240;
    private HttpVersionPolicy httpVersionPolicy = HttpVersionPolicy.NEGOTIATE;
    private boolean skipCipherCheck = false;

    public HttpClient5Builder setSslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    public HttpClient5Builder setRequestTimeOutInMilliseconds(int requestTimeOutInMilliseconds) {
      this.requestTimeOutInMilliseconds = requestTimeOutInMilliseconds;
      return this;
    }

    public HttpClient5Builder setIoThreadCount(int ioThreadCount) {
      this.ioThreadCount = ioThreadCount;
      return this;
    }

    public HttpClient5Builder setMaxTotalConnection(int maxTotalConnection) {
      this.maxTotalConnection = maxTotalConnection;
      return this;
    }

    public HttpClient5Builder setMaxConnectionPerRoute(int maxConnectionPerRoute) {
      this.maxConnectionPerRoute = maxConnectionPerRoute;
      return this;
    }

    public HttpClient5Builder setHttpVersionPolicy(HttpVersionPolicy httpVersionPolicy) {
      this.httpVersionPolicy = httpVersionPolicy;
      return this;
    }

    public HttpClient5Builder setSkipCipherCheck(boolean skipCipherCheck) {
      this.skipCipherCheck = skipCipherCheck;
      return this;
    }

    public MinimalHttpAsyncClient build() {
      if (sslContext == null) {
        throw new IllegalArgumentException("'sslContext' needs to be specified.");
      }
      final IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
          .setSoKeepAlive(true)
          .setTcpNoDelay(true)
          .setSoTimeout(Timeout.ofMilliseconds(requestTimeOutInMilliseconds))
          .setIoThreadCount(ioThreadCount)
          .build();

      final TlsStrategy tlsStrategy = skipCipherCheck ?
          VeniceClientTlsStrategyBuilder.create()
              .setSslContext(sslContext)
              .setTlsVersions(TLS.V_1_3, TLS.V_1_2)
              .build()
          : ClientTlsStrategyBuilder.create()
              .setSslContext(sslContext)
              .setTlsVersions(TLS.V_1_3, TLS.V_1_2)
              .build();

      final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
          .setTlsStrategy(tlsStrategy)
          .setMaxConnTotal(maxTotalConnection)
          .setMaxConnPerRoute(maxConnectionPerRoute)
          .build();

      final MinimalHttpAsyncClient client = HttpAsyncClients.createMinimal(httpVersionPolicy, H2Config.DEFAULT,
          Http1Config.DEFAULT, ioReactorConfig, connectionManager);

      client.start();

      return client;
    }
  }
}
