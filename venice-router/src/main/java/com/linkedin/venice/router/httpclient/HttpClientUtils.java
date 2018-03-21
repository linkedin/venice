package com.linkedin.venice.router.httpclient;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.router.stats.HttpConnectionPoolStats;
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
import org.apache.log4j.Logger;

import static com.linkedin.venice.HttpConstants.*;


public class HttpClientUtils {

  public static SSLIOSessionStrategy getSslStrategy(SSLEngineComponentFactory sslFactory) {
    SSLContext sslContext = sslFactory.getSSLContext();
    SSLIOSessionStrategy sslSessionStrategy = new SSLIOSessionStrategy(sslContext);
    return sslSessionStrategy;
  }

  public static CloseableHttpAsyncClient getMinimalHttpClient(int ioThreadNum, int maxConnPerRoute, int maxConnTotal,
      Optional<SSLEngineComponentFactory> sslFactory, Optional<CachedDnsResolver> dnsResolver, Optional<HttpConnectionPoolStats> poolStats) {
    PoolingNHttpClientConnectionManager connectionManager = createConnectionManager(ioThreadNum, maxConnPerRoute,
        maxConnTotal, sslFactory, dnsResolver, poolStats);
    if (poolStats.isPresent()) {
      poolStats.get().addConnectionPoolManager(connectionManager);
    }
    reapIdleConnections(connectionManager, 10, TimeUnit.MINUTES, 2, TimeUnit.HOURS);
    return HttpAsyncClients.createMinimal(connectionManager);
  }

  /**
   * Created a customized {@link DefaultConnectingIOReactor} to capture connection lease request latency.
   */
  private static class VeniceConnectingIOReactor extends DefaultConnectingIOReactor {
    private static Logger LOGGER = Logger.getLogger(VeniceConnectingIOReactor.class);
    private final Optional<HttpConnectionPoolStats> poolStats;

    private static class VeniceSessionRequestCallback implements SessionRequestCallback {
      private final long startTimeMs;
      private final SessionRequestCallback actualCallback;
      private final Optional<HttpConnectionPoolStats> poolStats;

      public VeniceSessionRequestCallback(SessionRequestCallback callback, Optional<HttpConnectionPoolStats> poolStats) {
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
        LOGGER.warn("Session request to " + request.getRemoteAddress() + " failed");
      }

      @Override
      public void timeout(SessionRequest request) {
        actualCallback.timeout(request);
        LOGGER.warn("Session request to " + request.getRemoteAddress() + " timed out");
      }

      @Override
      public void cancelled(SessionRequest request) {
        actualCallback.cancelled(request);
        LOGGER.warn("Session request to " + request.getRemoteAddress() + " cancelled");
      }
    }

    public VeniceConnectingIOReactor(final IOReactorConfig config, Optional<HttpConnectionPoolStats> poolStats) throws IOReactorException {
      super(config, null);
      this.poolStats = poolStats;
    }

    @Override
    public SessionRequest connect(
        SocketAddress remoteAddress,
        SocketAddress localAddress,
        Object attachment,
        SessionRequestCallback callback) {
      return super.connect(remoteAddress,
          localAddress,
          attachment,
          new VeniceSessionRequestCallback(callback, poolStats));
    }
  }

  public static PoolingNHttpClientConnectionManager createConnectionManager(int ioThreadNum, int perRoute, int total,
      Optional<SSLEngineComponentFactory> sslFactory, Optional<CachedDnsResolver> dnsResolver, Optional<HttpConnectionPoolStats> poolStats) {
    IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
        .setSoKeepAlive(true)
        .setIoThreadCount(ioThreadNum)
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
    if(sslFactory.isPresent()) {
      SSLIOSessionStrategy sslStrategy = getSslStrategy(sslFactory.get());
      registryBuilder.register(HTTPS, sslStrategy);
    }
    DnsResolver dnsResolverForConnectionManager = null;
    if (dnsResolver.isPresent()) {
      dnsResolverForConnectionManager = dnsResolver.get();
    }
    connMgr = new PoolingNHttpClientConnectionManager(ioReactor, null, registryBuilder.build(), dnsResolverForConnectionManager);
    connMgr.setMaxTotal(total);
    connMgr.setDefaultMaxPerRoute(perRoute);

    //TODO: Configurable
    reapIdleConnections(connMgr, 10, TimeUnit.MINUTES, 2, TimeUnit.HOURS);

    return connMgr;
  }

  /**
   * This function is only being used in RouterHeartbeat code, and we should not use cached dns resolver
   * since we would like to the unhealthy node to be reported correctly.
   * @param maxConnPerRoute
   * @param maxConnTotal
   * @param sslFactory
   * @return
   */
  public static CloseableHttpAsyncClient getMinimalHttpClient(int maxConnPerRoute, int maxConnTotal, Optional<SSLEngineComponentFactory> sslFactory) {
    return getMinimalHttpClient(1, maxConnPerRoute, maxConnTotal, sslFactory, Optional.empty(), Optional.empty());
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
  private static Thread reapIdleConnections(PoolingNHttpClientConnectionManager connectionManager,
      long sleepTime, TimeUnit sleepTimeUnits,
      long maxIdleTime, TimeUnit maxIdleTimeUnits) {
    Thread idleConnectionReaper = new Thread(()->{
      while (true){
        try {
          Thread.sleep(sleepTimeUnits.toMillis(sleepTime));
          connectionManager.closeIdleConnections(maxIdleTime, maxIdleTimeUnits);
        } catch (InterruptedException e){
          break;
        }
      }
    }, "ConnectionManagerIdleReaper");
    idleConnectionReaper.setDaemon(true);
    idleConnectionReaper.start();
    return idleConnectionReaper;
  }
}
