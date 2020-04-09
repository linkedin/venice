package com.linkedin.venice.router.httpclient;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.DnsLookupStats;
import com.linkedin.venice.router.stats.HttpConnectionPoolStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;

import static com.linkedin.venice.HttpConstants.*;


public class ApacheHttpAsyncStorageNodeClient implements StorageNodeClient  {
  private static final Logger logger = Logger.getLogger(ApacheHttpAsyncStorageNodeClient.class);
  private static final long CONNECTION_WARMING_WAIT_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1); // 1 mins.
  private static final long CONNECTION_WARMING_TOTAL_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5); // 5 mins

  private final String scheme;

  // see: https://hc.apache.org/httpcomponents-asyncclient-dev/quickstart.html
  private final int clientPoolSize;
  private final ArrayList<CloseableHttpAsyncClient> clientPool;
  private final Random random = new Random();
  private final VeniceConcurrentHashMap<String, CloseableHttpAsyncClient> hostToClientMap = new VeniceConcurrentHashMap<>();
  private final HttpConnectionPoolStats poolStats;
  private final LiveInstanceMonitor liveInstanceMonitor;
  private final VeniceRouterConfig routerConfig;

  private final boolean perNodeClientEnabled;
  private int ioThreadNumPerClient;
  private int maxConnPerRoutePerClient;
  private int totalMaxConnPerClient;
  private int socketTimeout;
  private int connectionTimeout;
  private Optional<SSLEngineComponentFactory> sslFactory;
  private Optional<CachedDnsResolver> dnsResolver = Optional.empty();

  public ApacheHttpAsyncStorageNodeClient(VeniceRouterConfig config, Optional<SSLEngineComponentFactory> sslFactory,
      MetricsRepository metricsRepository,
      LiveInstanceMonitor monitor) {
    this.scheme = sslFactory.isPresent() ? HTTPS_PREFIX : HTTP_PREFIX;

    int totalIOThreadNum = Runtime.getRuntime().availableProcessors();
    int maxConnPerRoute = config.getMaxOutgoingConnPerRoute();
    int maxConn = config.getMaxOutgoingConn();
    this.perNodeClientEnabled = config.isPerNodeClientAllocationEnabled();
    this.liveInstanceMonitor = monitor;
    this.poolStats = new HttpConnectionPoolStats(metricsRepository, "connection_pool");
    this.socketTimeout = config.getSocketTimeout();
    this.connectionTimeout = config.getConnectionTimeout();
    this.sslFactory = sslFactory;
    this.clientPoolSize = config.getHttpClientPoolSize();
    this.routerConfig = config;

    // Whether to enable DNS cache
    if (config.isDnsCacheEnabled()) {
      DnsLookupStats dnsLookupStats = new DnsLookupStats(metricsRepository, "dns_lookup");
      dnsResolver = Optional.of(new CachedDnsResolver(config.getHostPatternForDnsCache(), config.getDnsCacheRefreshIntervalInMs(), dnsLookupStats));
      logger.info("CachedDnsResolver is enabled, cached host pattern: " + config.getHostPatternForDnsCache() +
          ", refresh interval: " + config.getDnsCacheRefreshIntervalInMs() + "ms");
    }

    /**
     * Using a client pool to reduce lock contention introduced by {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager#requestConnection}
     * and {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager#releaseConnection}.
     */

    clientPool = new ArrayList<>();

    /**
     * We couldn't start multiple http clients in perNodeClient mode since {@link #liveInstanceMonitor} contains
     * nothing during construction.
     */
    if (! perNodeClientEnabled) {
      ioThreadNumPerClient = (int)Math.ceil(((double)totalIOThreadNum) / clientPoolSize);
      totalMaxConnPerClient = (int)Math.ceil(((double)maxConn) / clientPoolSize);
      maxConnPerRoutePerClient = (int)Math.ceil(((double)maxConnPerRoute) / clientPoolSize);

      for (int i = 0; i < clientPoolSize; ++i) {
        clientPool.add(createAndStartNewClient());
      }
    }
  }

  @Override
  public void start() {
    if (perNodeClientEnabled) {
      ioThreadNumPerClient = routerConfig.getPerNodeClientThreadCount();
      maxConnPerRoutePerClient = routerConfig.getMaxOutgoingConnPerRoute(); // Per host client gets the max config per client
      totalMaxConnPerClient = routerConfig.getMaxOutgoingConnPerRoute(); // Using the same maxConnPerRoute, may need to tune later.
      // TODO: clean up clients when host dies.
      Set<Instance> instanceSet = liveInstanceMonitor.getAllLiveInstances();
      int instanceNum = instanceSet.size();
      if (routerConfig.isHttpasyncclientConnectionWarmingEnabled()) {
        logger.info("Start creating clients and connection warming for " + instanceNum + " instances");
        ExecutorService executorService = Executors.newFixedThreadPool(instanceNum, new DaemonThreadFactory("HttpAsyncClient_ConnectionWarming_"));
        instanceSet.forEach(host -> executorService.submit( () -> {
              hostToClientMap.put(host.getHost(),
                  createAndWarmupNewClient(routerConfig.getHttpasyncclientConnectionWarmingSleepIntervalMs(), host));
            }));
        executorService.shutdown();
        try {
          executorService.awaitTermination(CONNECTION_WARMING_TOTAL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          throw new VeniceException("Failed to start HttpAsyncClient properly", e);
        }
        logger.info("Finished creating clients and connection warming for " + instanceNum + " instances");
      } else {
        logger.info("Connection warming is disabled in HttpAsyncClient");
        instanceSet.forEach(host -> hostToClientMap.put(host.getHost(),
                createAndStartNewClient()));
      }
    }
  }

  @Override
  public void close() {
    if (perNodeClientEnabled) {
      hostToClientMap.forEach((k,v) ->  IOUtils.closeQuietly(v));
    } else {
      clientPool.stream().forEach(client -> IOUtils.closeQuietly(client));
    }
  }

  @Override
  public void query(
      Instance host,
      VenicePath path,
      Consumer<PortableHttpResponse> completedCallBack,
      Consumer<Throwable> failedCallBack,
      BooleanSupplier cancelledCallBack,
      long queryStartTimeInNS) throws RouterException {
    /*
     * This function call is used to populate per-storage-node stats gradually since the connection pool
     * is empty at the very beginning.
     */
    poolStats.addStatsForRoute(host.getHost());

    //  http(s)://host:port/path
    String address = scheme + host.getHost() + ":" + host.getPort() + "/";
    final HttpUriRequest routerRequest = path.composeRouterRequest(address);
    CloseableHttpAsyncClient selectedClient;
    if (perNodeClientEnabled) {
      // If all the pool are used up by the set of live instances, spawn new client
      selectedClient = hostToClientMap.computeIfAbsent(host.getHost(), h-> createAndStartNewClient());
    } else {
      int selectedClientId = Math.abs(random.nextInt() % clientPoolSize);
      selectedClient = clientPool.get(selectedClientId);
    }

    selectedClient.execute(routerRequest, new HttpAsyncClientFutureCallBack(completedCallBack, failedCallBack, cancelledCallBack));
  }

  private CloseableHttpAsyncClient createAndWarmupNewClient(long connectionWarmingSleepIntervalMs, Instance host) {
    CloseableHttpAsyncClient client = createAndStartNewClient();
    String instanceUrl = host.getUrl(sslFactory.isPresent());
    logger.info("Started warming up " + maxConnPerRoutePerClient + " connections to server: " + instanceUrl);
    warmUpConnection(client, instanceUrl, maxConnPerRoutePerClient, connectionWarmingSleepIntervalMs);
    logger.info("Finished warming up " + maxConnPerRoutePerClient + " connections to server: "  + instanceUrl);

    return client;
  }

  private CloseableHttpAsyncClient createAndStartNewClient() {
    CloseableHttpAsyncClient client = HttpClientUtils.getMinimalHttpClient(ioThreadNumPerClient, maxConnPerRoutePerClient,
        totalMaxConnPerClient, socketTimeout, connectionTimeout, sslFactory, dnsResolver, Optional.of(poolStats),
        routerConfig.isIdleConnectionToServerCleanupEnabled(), routerConfig.getIdleConnectionToServerCleanupThresholdMins());
    client.start();
    return client;
  }

  private static class BlockingAsyncResponseConsumer extends BasicAsyncResponseConsumer {
    private final CountDownLatch latch;
    private final String instanceUrl;
    public BlockingAsyncResponseConsumer(CountDownLatch latch, String instanceUrl) {
      super();
      this.latch = latch;
      this.instanceUrl = instanceUrl;
    }

    @Override
    protected HttpResponse buildResult(final HttpContext context) {
      logger.info("Received buildResult invocation from instance url: " + instanceUrl);
      try {
        // Blocking IO threads of HttpAsyncClient, so that the current connection can't be reused by the new request
        latch.await();
      } catch (InterruptedException e) {
        throw new VeniceException("Encountered InterruptedException while awaiting", e);
      }
      return super.buildResult(context);
    }
  }
  /**
   * This function leverages {@link CloseableHttpAsyncClient#execute(HttpAsyncRequestProducer, HttpAsyncResponseConsumer, FutureCallback)} for connection warming up.
   * High-level ideas:
   * 1. This interface gives us capability to keep occupying one connection by blocking {@link HttpAsyncResponseConsumer#responseCompleted(HttpContext)};
   * 2. The connection warming response callback will be blocked until all of the new connection setup requests have been sent out;
   *    Check {@literal MinimalClientExchangeHandlerImpl#responseCompleted()} to find more details  about blocking logic;
   *    Check {@link org.apache.http.nio.pool.AbstractNIOConnPool#lease} to find more details about the connection setup logic;
   *
   * With this way, it is guaranteed that each request will trigger one new connection.
   */
  private void warmUpConnection(CloseableHttpAsyncClient client, String instanceUrl, int maxConnPerRoutePerClient, long connectionWarmingSleepIntervalMs) {
    FutureCallback<HttpResponse> dummyCallback = new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse result) {
      }

      @Override
      public void failed(Exception ex) {
      }

      @Override
      public void cancelled() {
      }
    };
    // Create a heart-beat request
    HttpAsyncRequestProducer requestProducer = new BasicAsyncRequestProducer(HttpHost.create(instanceUrl), new HttpGet(instanceUrl + "/" + QueryAction.HEALTH.toString().toLowerCase()));
    CountDownLatch latch = new CountDownLatch(1);
    List<Future> connectionWarmupResponseFutures = new ArrayList<>(maxConnPerRoutePerClient);
    try {
      for (int cur = 0; cur < maxConnPerRoutePerClient; ++cur) {
        Future responseFuture = client.execute(requestProducer, new BlockingAsyncResponseConsumer(latch, instanceUrl), dummyCallback);
        connectionWarmupResponseFutures.add(responseFuture);
        // To avoid overwhelming storage nodes
        Thread.sleep(connectionWarmingSleepIntervalMs);
      }
      /**
       * At this moment, all the new connection creation requests should be sent completely,
       * then we could let the response processing proceed.
        */
      latch.countDown();
      for (Future future : connectionWarmupResponseFutures) {
        /**
         * Waiting for up to {@link #CONNECTION_WARMING_WAIT_TIMEOUT_MS} to let new connection setup finish.
         * So that, all the new connections will be ready to use after.
         */
        future.get(CONNECTION_WARMING_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      throw new VeniceException("Encountered exception during connection warming to instance: " + instanceUrl, e);
    }
  }

  private static class HttpAsyncClientFutureCallBack implements FutureCallback<HttpResponse> {
    private final Consumer<PortableHttpResponse> completedCallBack;
    private final Consumer<Throwable> failedCallBack;
    private final BooleanSupplier cancelledCallBack;

    private HttpAsyncClientFutureCallBack(
        Consumer<PortableHttpResponse> completedCallBack,
        Consumer<Throwable> failedCallBack,
        BooleanSupplier cancelledCallBack) {
      this.completedCallBack = completedCallBack;
      this.failedCallBack = failedCallBack;
      this.cancelledCallBack = cancelledCallBack;
    }

    @Override
    public void completed(HttpResponse result) {
      completedCallBack.accept(new HttpAsyncClientPortableHttpResponse(result));
    }

    @Override
    public void failed(Exception ex) {
      failedCallBack.accept(ex);
    }

    @Override
    public void cancelled() {
      cancelledCallBack.getAsBoolean();
    }
  }

  private static class HttpAsyncClientPortableHttpResponse implements PortableHttpResponse {
    private final HttpResponse httpResponse;

    private HttpAsyncClientPortableHttpResponse(HttpResponse httpResponse) {
      this.httpResponse = httpResponse;
    }

    @Override
    public int getStatusCode() {
      return httpResponse.getStatusLine().getStatusCode();
    }

    @Override
    public byte[] getContentInBytes() throws IOException {
      byte[] contentToByte;
      try (InputStream contentStream = httpResponse.getEntity().getContent()) {
        contentToByte = IOUtils.toByteArray(contentStream);
      }
      return contentToByte;
    }

    @Override
    public boolean containsHeader(String headerName) {
      return httpResponse.containsHeader(headerName);
    }

    @Override
    public String getFirstHeader(String headerName) {
      Header header = httpResponse.getFirstHeader(headerName);
      return header != null ? header.getValue() : null;
    }
  }
}
