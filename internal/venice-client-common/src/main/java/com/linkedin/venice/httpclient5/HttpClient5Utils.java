package com.linkedin.venice.httpclient5;

import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.impl.DefaultConnectionReuseStrategy;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;


/**
 * This class is used to provide a facility to initialize httpclient5 lib based Http/2 Client.
 * Currently, the compatible API offered by httpclient5 (https://hc.apache.org/httpcomponents-client-5.1.x/migration-guide/migration-to-async-streaming.html),
 * which supports both http/1.1 and http/2 is buggy since it couldn't recover from a crashed peer,
 * and here is the behavior when the peer crashes:
 * 1. The H2 connection created by httpclient5 won't terminate on its own.
 * 2. After the peer is back, the compatible API will continue to use the same connection, which will stuck.
 *
 * There is one constraint with the httpclient5 H2 specific API:
 * https://hc.apache.org/httpcomponents-client-5.1.x/migration-guide/migration-to-async-http2.html
 * and the H2 initial window size of the peer needs to be 65535 or lower.
 *
 * For now, if we want to use http/1.1, we will need to use httpasyncclient-4.x directly.
 *
 * TODO: follow up with the httpclient team to get a proper fix.
 */
public class HttpClient5Utils {
  public static class HttpClient5Builder {
    private SSLContext sslContext;
    private long requestTimeOutInMilliseconds = TimeUnit.SECONDS.toMillis(1); // 1s by default
    /**
     * We need to use a high connect timeout to avoid reconnect issue, which might result in confusing logging and unhealthy requests.
     * For now, we remove the functions updating to connect timeout to avoid mistakes.
     */
    private final Timeout CONNECT_TIMEOUT_IN_MINUTES = Timeout.ofMinutes(1);
    private final Timeout CONNECTION_KEEP_ALIVE_TIMEOUT_IN_HOURS = Timeout.ofHours(1);
    private final Timeout SOCKET_INDEFINITE_TIMEOUT = Timeout.ZERO_MILLISECONDS;

    private int ioThreadCount = 48;
    private boolean skipCipherCheck = false;

    private boolean http1 = false;
    private int http1MaxConnectionsTotal = 0;
    private int http1MaxConnectionsPerRoute = 0;

    public HttpClient5Builder setSslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    public HttpClient5Builder setRequestTimeOutInMilliseconds(long requestTimeOutInMilliseconds) {
      this.requestTimeOutInMilliseconds = requestTimeOutInMilliseconds;
      return this;
    }

    public HttpClient5Builder setIoThreadCount(int ioThreadCount) {
      this.ioThreadCount = ioThreadCount;
      return this;
    }

    public HttpClient5Builder setSkipCipherCheck(boolean skipCipherCheck) {
      this.skipCipherCheck = skipCipherCheck;
      return this;
    }

    public HttpClient5Builder setHttp1(boolean http1) {
      this.http1 = http1;
      return this;
    }

    public HttpClient5Builder setHttp1MaxConnectionsTotal(int http1MaxConnectionsTotal) {
      this.http1MaxConnectionsTotal = http1MaxConnectionsTotal;
      return this;
    }

    public HttpClient5Builder setHttp1MaxConnectionsPerRoute(int http1MaxConnectionsPerRoute) {
      this.http1MaxConnectionsPerRoute = http1MaxConnectionsPerRoute;
      return this;
    }

    private ConnectionConfig getDefaultConnectionConfig() {
      return ConnectionConfig.custom()
          .setConnectTimeout(CONNECT_TIMEOUT_IN_MINUTES)
          /**
           * To not close the connection because of inactivity, we set the socket timeout to 0.
           * check {@link org.apache.hc.core5.reactor.IOSession#setSocketTimeout} for more details
           */
          .setSocketTimeout(SOCKET_INDEFINITE_TIMEOUT)
          .build();
    }

    private RequestConfig getDefaultRequestConfig() {
      return RequestConfig.custom()
          .setResponseTimeout(Timeout.ofMilliseconds(requestTimeOutInMilliseconds))
          .setConnectionRequestTimeout(CONNECT_TIMEOUT_IN_MINUTES)
          // Override default keep alive time of 3 minutes to CONNECTION_KEEP_ALIVE_TIMEOUT_IN_HOURS
          .setConnectionKeepAlive(CONNECTION_KEEP_ALIVE_TIMEOUT_IN_HOURS)
          .build();
    }

    public CloseableHttpAsyncClient build() {
      if (sslContext == null && !http1) {
        throw new IllegalArgumentException("'sslContext' needs to be specified.");
      }
      final IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
          .setSoKeepAlive(true)
          .setTcpNoDelay(true)
          .setSoTimeout(Timeout.ofMilliseconds(requestTimeOutInMilliseconds))
          .setIoThreadCount(ioThreadCount)
          .build();

      final TlsStrategy tlsStrategy = sslContext == null
          ? null
          : skipCipherCheck
              ? VeniceClientTlsStrategyBuilder.create()
                  .setSslContext(sslContext)
                  .setTlsVersions(TLS.V_1_3, TLS.V_1_2)
                  .build()
              : ClientTlsStrategyBuilder.create()
                  .setSslContext(sslContext)
                  .setTlsVersions(TLS.V_1_3, TLS.V_1_2)
                  .build();

      if (http1) {
        return HttpAsyncClients.custom()
            .setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1)
            .setIOReactorConfig(ioReactorConfig)
            .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
            .setConnectionManager(
                PoolingAsyncClientConnectionManagerBuilder.create()
                    .setMaxConnTotal(http1MaxConnectionsTotal)
                    .setMaxConnPerRoute(http1MaxConnectionsPerRoute)
                    .setTlsStrategy(tlsStrategy)
                    .setDefaultConnectionConfig(getDefaultConnectionConfig())
                    .build())
            .setDefaultRequestConfig(getDefaultRequestConfig())
            .setUserTokenHandler((route, context) -> null)
            .build();
      } else {
        return HttpAsyncClients.customHttp2()
            .setTlsStrategy(tlsStrategy)
            .setIOReactorConfig(ioReactorConfig)
            .setDefaultConnectionConfig(getDefaultConnectionConfig())
            .setDefaultRequestConfig(getDefaultRequestConfig())
            .build();
      }
    }

    public CloseableHttpAsyncClient buildAndStart() {
      CloseableHttpAsyncClient client = build();
      client.start();
      return client;
    }
  }
}
