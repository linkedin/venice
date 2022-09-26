package com.linkedin.venice.httpclient;

import static com.linkedin.venice.HttpConstants.HTTP;
import static com.linkedin.venice.HttpConstants.HTTPS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.stats.HttpConnectionPoolStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.nio.reactor.SessionRequest;
import org.apache.http.nio.reactor.SessionRequestCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HttpClientUtils {
  private static final Logger LOGGER = LogManager.getLogger(HttpClientUtils.class);

  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  public static class ClosableHttpAsyncClientWithConnManager {
    private final CloseableHttpAsyncClient client;
    private final PoolingNHttpClientConnectionManager connManager;

    public ClosableHttpAsyncClientWithConnManager(
        CloseableHttpAsyncClient client,
        PoolingNHttpClientConnectionManager connManager) {
      this.client = client;
      this.connManager = connManager;
    }

    public CloseableHttpAsyncClient getClient() {
      return client;
    }

    public PoolingNHttpClientConnectionManager getConnManager() {
      return connManager;
    }
  }

  public static SSLIOSessionStrategy getSslStrategy(SSLFactory sslFactory) {
    SSLContext sslContext = sslFactory.getSSLContext();
    SSLIOSessionStrategy sslSessionStrategy = new SSLIOSessionStrategy(sslContext);
    return sslSessionStrategy;
  }

  public static CloseableHttpAsyncClient getMinimalHttpClient(
      int ioThreadNum,
      int maxConnPerRoute,
      int maxConnTotal,
      int socketTimeout,
      int connectionTimeout,
      Optional<SSLFactory> sslFactory,
      Optional<CachedDnsResolver> dnsResolver,
      Optional<HttpConnectionPoolStats> poolStats) {
    return getMinimalHttpClient(
        ioThreadNum,
        maxConnPerRoute,
        maxConnTotal,
        socketTimeout,
        connectionTimeout,
        sslFactory,
        dnsResolver,
        poolStats,
        true,
        TimeUnit.HOURS.toMinutes(3));
  }

  public static ClosableHttpAsyncClientWithConnManager getMinimalHttpClientWithConnManager(
      int ioThreadNum,
      int maxConnPerRoute,
      int maxConnTotal,
      int socketTimeout,
      int connectionTimeout,
      Optional<SSLFactory> sslFactory,
      Optional<CachedDnsResolver> dnsResolver,
      Optional<HttpConnectionPoolStats> poolStats,
      boolean isIdleConnectionToServerCleanupEnabled,
      long idleConnectionCleanupThresholdMins) {
    PoolingNHttpClientConnectionManager connectionManager = createConnectionManager(
        ioThreadNum,
        maxConnPerRoute,
        maxConnTotal,
        socketTimeout,
        connectionTimeout,
        sslFactory,
        dnsResolver,
        poolStats);
    if (poolStats.isPresent()) {
      poolStats.get().addConnectionPoolManager(connectionManager);
    }
    if (isIdleConnectionToServerCleanupEnabled) {
      LOGGER.info(
          "Idle connection to server cleanup is enabled, and the idle threshold is {} mins",
          idleConnectionCleanupThresholdMins);
      reapIdleConnections(
          connectionManager,
          10,
          TimeUnit.MINUTES,
          idleConnectionCleanupThresholdMins,
          TimeUnit.MINUTES);
    } else {
      LOGGER.info("Idle connection to server cleanup is disabled");
    }
    return new ClosableHttpAsyncClientWithConnManager(
        HttpAsyncClients.createMinimal(connectionManager),
        connectionManager);
  }

  public static CloseableHttpAsyncClient getMinimalHttpClient(
      int ioThreadNum,
      int maxConnPerRoute,
      int maxConnTotal,
      int socketTimeout,
      int connectionTimeout,
      Optional<SSLFactory> sslFactory,
      Optional<CachedDnsResolver> dnsResolver,
      Optional<HttpConnectionPoolStats> poolStats,
      boolean isIdleConnectionToServerCleanupEnabled,
      long idleConnectionCleanupThresholdMins) {
    return getMinimalHttpClientWithConnManager(
        ioThreadNum,
        maxConnPerRoute,
        maxConnTotal,
        socketTimeout,
        connectionTimeout,
        sslFactory,
        dnsResolver,
        poolStats,
        isIdleConnectionToServerCleanupEnabled,
        idleConnectionCleanupThresholdMins).getClient();
  }

  /**
   * Created a customized {@link DefaultConnectingIOReactor} to capture connection lease request latency.
   */
  private static class VeniceConnectingIOReactor extends DefaultConnectingIOReactor {
    private static final Logger LOGGER = LogManager.getLogger(VeniceConnectingIOReactor.class);
    private final Optional<HttpConnectionPoolStats> poolStats;

    private static class VeniceSessionRequestCallback implements SessionRequestCallback {
      private final long startTimeMs;
      private final SessionRequestCallback actualCallback;
      private final Optional<HttpConnectionPoolStats> poolStats;

      public VeniceSessionRequestCallback(
          SessionRequestCallback callback,
          Optional<HttpConnectionPoolStats> poolStats) {
        this.startTimeMs = System.currentTimeMillis();
        this.actualCallback = callback;
        this.poolStats = poolStats;
      }

      @Override
      public void completed(SessionRequest request) {
        actualCallback.completed(request);
        if (poolStats.isPresent()) {
          poolStats.get().recordConnectionLeaseRequestLatency(System.currentTimeMillis() - startTimeMs);
        }
      }

      @Override
      public void failed(SessionRequest request) {
        actualCallback.failed(request);
        logSessionFailure(request.getRemoteAddress().toString(), request.getException(), "failed");
      }

      @Override
      public void timeout(SessionRequest request) {
        actualCallback.timeout(request);
        logSessionFailure(request.getRemoteAddress().toString(), request.getException(), "timeout");
      }

      @Override
      public void cancelled(SessionRequest request) {
        actualCallback.cancelled(request);
        logSessionFailure(request.getRemoteAddress().toString(), request.getException(), "cancelled");
      }

      /**
       * Log the actual exception when there is a failure in the channel between router and server; only log
       * the same exception once a minute to avoid flooding the router log.
       *
       * @param remoteHost
       * @param e
       * @param failureType
       */
      private static void logSessionFailure(String remoteHost, Exception e, String failureType) {
        if (e != null) {
          if (!EXCEPTION_FILTER.isRedundantException(remoteHost, e)) {
            LOGGER.warn("Session request to {} {}: ", remoteHost, failureType, e);
          }
        } else {
          LOGGER.warn("Session request to {} {}", remoteHost, failureType);
        }
      }
    }

    public VeniceConnectingIOReactor(final IOReactorConfig config, Optional<HttpConnectionPoolStats> poolStats)
        throws IOReactorException {
      super(config, null);
      this.poolStats = poolStats;
    }

    @Override
    public SessionRequest connect(
        SocketAddress remoteAddress,
        SocketAddress localAddress,
        Object attachment,
        SessionRequestCallback callback) {
      return super.connect(
          remoteAddress,
          localAddress,
          attachment,
          new VeniceSessionRequestCallback(callback, poolStats));
    }
  }

  public static PoolingNHttpClientConnectionManager createConnectionManager(
      int ioThreadNum,
      int perRoute,
      int total,
      int soTimeout,
      int connectionTimeout,
      Optional<SSLFactory> sslFactory,
      Optional<CachedDnsResolver> dnsResolver,
      Optional<HttpConnectionPoolStats> poolStats) {
    IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
        .setSoKeepAlive(true)
        .setIoThreadCount(ioThreadNum)
        .setSoTimeout(soTimeout)
        .setConnectTimeout(connectionTimeout)
        .build();
    ConnectingIOReactor ioReactor = null;
    try {
      ioReactor = new VeniceConnectingIOReactor(ioReactorConfig, poolStats);
    } catch (IOReactorException e) {
      throw new VeniceException("Router failed to create an IO Reactor", e);
    }
    PoolingNHttpClientConnectionManager connMgr;
    RegistryBuilder<SchemeIOSessionStrategy> registryBuilder = RegistryBuilder.create();
    registryBuilder.register(HTTP, NoopIOSessionStrategy.INSTANCE);
    if (sslFactory.isPresent()) {
      SSLIOSessionStrategy sslStrategy = getSslStrategy(sslFactory.get());
      registryBuilder.register(HTTPS, sslStrategy);
    }
    DnsResolver dnsResolverForConnectionManager = null;
    if (dnsResolver.isPresent()) {
      dnsResolverForConnectionManager = dnsResolver.get();
    }
    connMgr = new PoolingNHttpClientConnectionManager(
        ioReactor,
        null,
        registryBuilder.build(),
        dnsResolverForConnectionManager);
    connMgr.setMaxTotal(total);
    connMgr.setDefaultMaxPerRoute(perRoute);

    return connMgr;
  }

  /**
   * This function is only being used in test cases.
   * @param maxConnPerRoute
   * @param maxConnTotal
   * @param sslFactory
   * @return
   */
  public static CloseableHttpAsyncClient getMinimalHttpClient(
      int maxConnPerRoute,
      int maxConnTotal,
      Optional<SSLFactory> sslFactory) {
    return getMinimalHttpClient(
        1,
        maxConnPerRoute,
        maxConnTotal,
        10000,
        10000,
        sslFactory,
        Optional.empty(),
        Optional.empty(),
        true,
        TimeUnit.HOURS.toMinutes(3));
  }

  /**
   * Creates a new thread that automatically cleans up idle connections on the specified connection manager.
   * @param connectionManager  Connection manager with idle connections that should be reaped
   * @param sleepTime how frequently to wake up and reap idle connections
   * @param sleepTimeUnits
   * @param maxIdleTime how long a connection must be idle in order to be eligible for reaping
   * @param maxIdleTimeUnits
   * @return started daemon thread that is doing the reaping.  Interrupt this thread to halt reaping or ignore the return value.
   */
  private static Thread reapIdleConnections(
      PoolingNHttpClientConnectionManager connectionManager,
      long sleepTime,
      TimeUnit sleepTimeUnits,
      long maxIdleTime,
      TimeUnit maxIdleTimeUnits) {
    Thread idleConnectionReaper = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(sleepTimeUnits.toMillis(sleepTime));
          connectionManager.closeIdleConnections(maxIdleTime, maxIdleTimeUnits);
        } catch (InterruptedException e) {
          break;
        }
      }
    }, "ConnectionManagerIdleReaper");
    idleConnectionReaper.setDaemon(true);
    idleConnectionReaper.start();
    return idleConnectionReaper;
  }
}
