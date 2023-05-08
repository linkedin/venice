package com.linkedin.venice.router.httpclient;

import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.httpclient.CachedDnsResolver;
import com.linkedin.venice.httpclient.HttpClientUtils;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceChangedListener;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.stats.DnsLookupStats;
import com.linkedin.venice.stats.HttpConnectionPoolStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.pool.PoolStats;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ApacheHttpAsyncStorageNodeClient implements StorageNodeClient {
  private static final Logger LOGGER = LogManager.getLogger(ApacheHttpAsyncStorageNodeClient.class);

  // see: https://hc.apache.org/httpcomponents-asyncclient-dev/quickstart.html
  private final int clientPoolSize;
  private final ArrayList<CloseableHttpAsyncClient> clientPool;
  private final Random random = new Random();
  private final Map<String, HttpClientUtils.ClosableHttpAsyncClientWithConnManager> nodeIdToClientMap =
      new VeniceConcurrentHashMap<>();
  private final HttpConnectionPoolStats poolStats;
  private final LiveInstanceMonitor liveInstanceMonitor;
  private final VeniceRouterConfig routerConfig;
  private final boolean perNodeClientEnabled;
  private final int socketTimeout;
  private final int connectionTimeout;
  private final Optional<SSLFactory> sslFactory;

  private int ioThreadNumPerClient;
  private int maxConnPerRoutePerClient;
  private int totalMaxConnPerClient;
  private Optional<CachedDnsResolver> dnsResolver = Optional.empty();
  private ClientConnectionWarmingService clientConnectionWarmingService = null;

  public ApacheHttpAsyncStorageNodeClient(
      VeniceRouterConfig config,
      Optional<SSLFactory> sslFactory,
      MetricsRepository metricsRepository,
      LiveInstanceMonitor monitor) {

    int totalIOThreadNum = config.getIoThreadCountInPoolMode();
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
      dnsResolver = Optional.of(
          new CachedDnsResolver(
              config.getHostPatternForDnsCache(),
              config.getDnsCacheRefreshIntervalInMs(),
              dnsLookupStats));
      LOGGER.info(
          "CachedDnsResolver is enabled, cached host pattern: {}, refresh interval: {}ms",
          config.getHostPatternForDnsCache(),
          config.getDnsCacheRefreshIntervalInMs());
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
    if (!perNodeClientEnabled) {
      ioThreadNumPerClient = (int) Math.ceil(((double) totalIOThreadNum) / clientPoolSize);
      totalMaxConnPerClient = (int) Math.ceil(((double) maxConn) / clientPoolSize);
      maxConnPerRoutePerClient = (int) Math.ceil(((double) maxConnPerRoute) / clientPoolSize);

      for (int i = 0; i < clientPoolSize; ++i) {
        clientPool.add(createAndStartNewClient().getClient());
      }
    }
  }

  public CloseableHttpAsyncClient getHttpClientForHost(String host) {
    if (perNodeClientEnabled) {
      if (nodeIdToClientMap.containsKey(host)) {
        return nodeIdToClientMap.get(host).getClient();
      } else {
        return null;
      }
    }
    int selectedClientId = Math.abs(random.nextInt() % clientPool.size());
    return clientPool.get(selectedClientId);
  }

  @Override
  public void start() {
    if (perNodeClientEnabled) {
      ioThreadNumPerClient = routerConfig.getPerNodeClientThreadCount();
      maxConnPerRoutePerClient = routerConfig.getMaxOutgoingConnPerRoute(); // Per host client gets the max config per
                                                                            // client
      totalMaxConnPerClient = routerConfig.getMaxOutgoingConnPerRoute(); // Using the same maxConnPerRoute, may need to
                                                                         // tune later.
      // TODO: clean up clients when host dies.
      Set<Instance> instanceSet = liveInstanceMonitor.getAllLiveInstances();
      instanceSet.forEach(host -> nodeIdToClientMap.put(host.getNodeId(), createAndStartNewClient()));
      if (routerConfig.isHttpasyncclientConnectionWarmingEnabled()) {
        LOGGER.info("Connection warming is enabled in HttpAsyncClient");
        clientConnectionWarmingService = new ClientConnectionWarmingService();
        clientConnectionWarmingService.start();
      } else {
        LOGGER.info("Connection warming is disabled in HttpAsyncClient");
      }
    }
  }

  @Override
  public boolean isInstanceReadyToServe(String instanceId) {
    return clientConnectionWarmingService == null || clientConnectionWarmingService.isInstanceReadyToServe(instanceId);
  }

  @Override
  public void close() {
    if (perNodeClientEnabled) {
      nodeIdToClientMap.forEach((k, v) -> Utils.closeQuietlyWithErrorLogged(v.getClient()));
      if (clientConnectionWarmingService != null) {
        try {
          clientConnectionWarmingService.stop();
        } catch (Exception e) {
          LOGGER.error("Received exception when stopping ClientConnectionWarmingService", e);
        }
      }
    } else {
      clientPool.stream().forEach(client -> Utils.closeQuietlyWithErrorLogged(client));
    }
  }

  /**
   * {@link ClientConnectionWarmingService} will take care of 3 different cases:
   * 1. Router restart, and it will warm up the clients to all the live instances in {@link #startInner()};
   * 2. Storage Node restart, and it will monitor the live instance change and warm up a new client for the new instances;
   * 3. Connection pool maintenance. A {@link #clientConnHealthinessScannerThread} will periodically scan all the
   *    existing clients and if the number of available connections of some client is below {@link #connectionWarmingLowWaterMark},
   *    it will try to create a new client and warm it up to {@link #maxConnPerRoutePerClient} connections and replace it;
   *
   * More details:
   * For #1, it is quite clear, and if the connection warming of the clients to all the live instances couldn't be accomplished
   * within {@link #CONNECTION_WARMING_TOTAL_TIMEOUT_MS}, it will throw an exception to let Router fail fast.
   *
   * For #2, Router will delay the new instance until the timeout threshold: {@link #newInstanceDelayJoinMs} or the connection
   * warming is done before the timeout. Since connection warming is a best-effort, we would like to prioritize the availability.
   *
   * For #3, Whenever the {@link #clientConnHealthinessScannerThread} finds a candidate, it will try to create a new client
   * and warm it up and swap with the old client, and the old client will be gracefully shutdown by the pre-defined delay:
   * {@link #CLIENT_GRACEFUL_SHUTDOWN_DELAY_IN_MS}. So far, we haven't found a way to do connection warming with the existing
   * {@link CloseableHttpAsyncClient} reliably without affecting the live traffic.
   */
  private class ClientConnectionWarmingService extends AbstractVeniceService {
    private static final String CONNECTION_WARMING_THREAD_PREFIX = "HttpAsyncClient_ConnectionWarming_";

    private final long CONNECTION_WARMING_WAIT_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1); // 1 mins.
    private final long CONNECTION_WARMING_TOTAL_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5); // 5 mins
    private final long CONNECTION_WARMING_SCANNER_SLEEP_INTERVAL_IN_MS = TimeUnit.MINUTES.toMillis(5); // 5 mins
    private final long CLIENT_GRACEFUL_SHUTDOWN_DELAY_IN_MS = TimeUnit.MINUTES.toMillis(3); // 3 mins

    private final Set<String> ongoingWarmUpClientSet = new HashSet<>();
    private final Map<CloseableHttpAsyncClient, Long> clientToCloseTimestampMap = new VeniceConcurrentHashMap<>();
    private final ExecutorService clientConnWarmingExecutor;
    private final int connectionWarmingLowWaterMark;
    private final long newInstanceDelayJoinMs;
    private final Thread clientConnHealthinessScannerThread;
    /**
     * This map contains the mapping between the new added instances detected by {@link com.linkedin.venice.helix.HelixLiveInstanceMonitor}
     * and the force join timestamp (if the connection warming takes longer time than expected), Router will force
     * the new instance join the online traffic serving.
     */
    private final Map<String, Long> nodeIdToForceJoinTimeMap = new VeniceConcurrentHashMap<>();

    private boolean clientConnHealthinessScannerStopped = false;

    public ClientConnectionWarmingService() {
      // Initialize the ongoing client warming executor
      this.clientConnWarmingExecutor = Executors.newFixedThreadPool(
          routerConfig.getHttpasyncclientConnectionWarmingExecutorThreadNum(),
          new DaemonThreadFactory(CONNECTION_WARMING_THREAD_PREFIX));
      this.connectionWarmingLowWaterMark = routerConfig.getHttpasyncclientConnectionWarmingLowWaterMark();
      if (connectionWarmingLowWaterMark > maxConnPerRoutePerClient) {
        throw new VeniceException(
            "Connection warming low water mark: " + connectionWarmingLowWaterMark
                + " shouldn't be higher than the max connection per client: " + maxConnPerRoutePerClient);
      }
      this.newInstanceDelayJoinMs = routerConfig.getHttpasyncclientConnectionWarmingNewInstanceDelayJoinMs();
      this.clientConnHealthinessScannerThread =
          new Thread(new ClientConnHealthinessScanner(), CONNECTION_WARMING_THREAD_PREFIX + "scanner");
    }

    public boolean isInstanceReadyToServe(String instanceId) {
      Long forceJoinTimestamp = nodeIdToForceJoinTimeMap.get(instanceId);
      if (forceJoinTimestamp == null) {
        return true;
      }
      if (forceJoinTimestamp < System.currentTimeMillis()) {
        nodeIdToForceJoinTimeMap.remove(instanceId);
        return true;
      }
      return false;
    }

    @Override
    public boolean startInner() throws Exception {
      int instanceNum = nodeIdToClientMap.size();
      logger.info("Start connection warming for {} instances", instanceNum);
      if (instanceNum == 0) {
        return true;
      }
      /**
       * This is for one-time use during start, and we would like to warm up the connections to all the instances
       * as fast as possible.
       */
      ExecutorService clientConnectionWarmingExecutorDuringStart =
          Executors.newFixedThreadPool(instanceNum, new DaemonThreadFactory(CONNECTION_WARMING_THREAD_PREFIX));

      List<CompletableFuture<?>> futureList = new ArrayList<>(instanceNum);
      nodeIdToClientMap.forEach((nodeId, clientWithConnManager) -> {
        futureList.add(CompletableFuture.runAsync(() -> {
          Instance instance = Instance.fromNodeId(nodeId);
          String instanceUrl = instance.getUrl(sslFactory.isPresent());
          logger.info("Started warming up {} connections to server: {}", maxConnPerRoutePerClient, instanceUrl);
          warmUpConnection(
              clientWithConnManager.getClient(),
              instanceUrl,
              maxConnPerRoutePerClient,
              routerConfig.getHttpasyncclientConnectionWarmingSleepIntervalMs());
          logger.info("Finished warming up {} connections to server: {}", maxConnPerRoutePerClient, instanceUrl);
        }, clientConnectionWarmingExecutorDuringStart));
      });
      CompletableFuture[] futureArray = new CompletableFuture[futureList.size()];
      CompletableFuture allFuture = CompletableFuture.allOf(futureList.toArray(futureArray));
      try {
        allFuture.get(CONNECTION_WARMING_TOTAL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        logger.info("Finished connection warming for {} instances", instanceNum);
      } catch (Exception e) {
        throw new VeniceException("Failed to warm up HttpAsyncClient properly", e);
      } finally {
        clientConnectionWarmingExecutorDuringStart.shutdown();
      }

      // Start scanner thread
      clientConnHealthinessScannerThread.start();

      // Register new instance callback
      liveInstanceMonitor.registerLiveInstanceChangedListener(new LiveInstanceChangedListener() {
        @Override
        public void handleNewInstances(Set<Instance> newInstances) {
          long currentTimestamp = System.currentTimeMillis();
          newInstances.forEach(instance -> {
            nodeIdToForceJoinTimeMap.put(instance.getNodeId(), currentTimestamp + newInstanceDelayJoinMs);
            logger.info("Create and warm up a new http async client for instance: {}", instance);
            asyncCreateAndWarmupNewClientAndSwapAsync(instance, true);
          });
        }

        @Override
        public void handleDeletedInstances(Set<Instance> deletedInstances) {
          /**
           * Whether we need to clean up the client for the deleted instances.
           * It seems to be risky since the deletion could be caused by the unstable ZK conn.
           * TODO: explore a better way to cleanup unused clients.
           */
        }
      });

      return true;
    }

    private class ClientConnHealthinessScanner implements Runnable {
      @Override
      public void run() {
        while (!clientConnHealthinessScannerStopped) {
          try {
            Thread.sleep(CONNECTION_WARMING_SCANNER_SLEEP_INTERVAL_IN_MS);

            // First close the deprecated clients
            clientToCloseTimestampMap.forEach((client, closeTimestamp) -> {
              if (closeTimestamp <= System.currentTimeMillis()) {
                try {
                  client.close();
                } catch (Exception e) {
                  LOGGER.warn("Failed to close an HttpAsyncClient properly", e);
                }
              }
            });

            // Then scan the existing clients
            nodeIdToClientMap.forEach((nodeId, clientWithConnManager) -> {
              Instance currentInstance = Instance.fromNodeId(nodeId);
              if (!liveInstanceMonitor.isInstanceAlive(currentInstance)) {
                // Not alive right now
                return;
              }
              PoolStats stats = clientWithConnManager.getConnManager().getTotalStats();
              int availableConnections = stats.getAvailable() + stats.getLeased();
              if (availableConnections < connectionWarmingLowWaterMark) {
                logger.info(
                    "Create a new HttpAsyncClient and warm it up for instance: {} since the total available connections: {} is lower than connection warming low water mark: {}",
                    currentInstance,
                    availableConnections,
                    connectionWarmingLowWaterMark);
                asyncCreateAndWarmupNewClientAndSwapAsync(currentInstance, false);
              }
            });
          } catch (InterruptedException e) {
            LOGGER.info("Received InterruptedException in ClientConnHealthinessScanner, will exit");
            break;
          }
        }
      }
    }

    private synchronized void asyncCreateAndWarmupNewClientAndSwapAsync(Instance instance, boolean force) {
      String nodeId = instance.getNodeId();
      if (!force && ongoingWarmUpClientSet.contains(nodeId)) {
        logger.info(
            "Connection warming for instance: {} has already stared, so the new connection warming request will be skipped",
            instance);
        return;
      }
      ongoingWarmUpClientSet.add(nodeId);
      clientConnWarmingExecutor.submit(() -> {
        try {
          HttpClientUtils.ClosableHttpAsyncClientWithConnManager newClientWithConnManager =
              createAndWarmupNewClient(routerConfig.getHttpasyncclientConnectionWarmingSleepIntervalMs(), instance);
          // create new client
          HttpClientUtils.ClosableHttpAsyncClientWithConnManager existingClientWithConnManager =
              nodeIdToClientMap.get(nodeId);
          if (existingClientWithConnManager != null) {
            // Remove the conn manager of the deprecated client from stats
            poolStats.removeConnectionPoolManager(existingClientWithConnManager.getConnManager());
            // Gracefully shutdown the current client
            clientToCloseTimestampMap.put(
                existingClientWithConnManager.getClient(),
                System.currentTimeMillis() + CLIENT_GRACEFUL_SHUTDOWN_DELAY_IN_MS);
          }
          nodeIdToClientMap.put(nodeId, newClientWithConnManager);
        } finally {
          // No matter the connection warming succeeds or not, we need to clean it up to allow the retry in the next
          // round if needed.
          ongoingWarmUpClientSet.remove(nodeId);
          if (force) {
            nodeIdToForceJoinTimeMap.remove(nodeId);
          }
        }
      });
    }

    private HttpClientUtils.ClosableHttpAsyncClientWithConnManager createAndWarmupNewClient(
        long connectionWarmingSleepIntervalMs,
        Instance host) {
      HttpClientUtils.ClosableHttpAsyncClientWithConnManager clientWithConnManager = createAndStartNewClient();
      String instanceUrl = host.getUrl(sslFactory.isPresent());
      logger.info("Started warming up {} connections to server: {}", maxConnPerRoutePerClient, instanceUrl);
      try {
        warmUpConnection(
            clientWithConnManager.getClient(),
            instanceUrl,
            maxConnPerRoutePerClient,
            connectionWarmingSleepIntervalMs);
        logger.info("Finished warming up {} connections to server: {}", maxConnPerRoutePerClient, instanceUrl);
      } catch (Exception e) {
        Utils.closeQuietlyWithErrorLogged(clientWithConnManager.getClient());
        throw new VeniceException(
            "Received exception while warming up " + maxConnPerRoutePerClient + " connections to server: " + instanceUrl
                + ", and closed the new created httpasyncclient, and exception: " + e);
      }
      return clientWithConnManager;
    }

    private class BlockingAsyncResponseConsumer extends BasicAsyncResponseConsumer {
      private final CountDownLatch latch;
      private final String instanceUrl;

      public BlockingAsyncResponseConsumer(CountDownLatch latch, String instanceUrl) {
        super();
        this.latch = latch;
        this.instanceUrl = instanceUrl;
      }

      @Override
      protected HttpResponse buildResult(final HttpContext context) {
        logger.info("Received buildResult invocation from instance url: {}", instanceUrl);
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
    private void warmUpConnection(
        CloseableHttpAsyncClient client,
        String instanceUrl,
        int maxConnPerRoutePerClient,
        long connectionWarmingSleepIntervalMs) {
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
      HttpAsyncRequestProducer requestProducer = new BasicAsyncRequestProducer(
          HttpHost.create(instanceUrl),
          new HttpGet(instanceUrl + "/" + QueryAction.HEALTH.toString().toLowerCase()));
      CountDownLatch latch = new CountDownLatch(1);
      List<Future> connectionWarmupResponseFutures = new ArrayList<>(maxConnPerRoutePerClient);
      try {
        for (int cur = 0; cur < maxConnPerRoutePerClient; ++cur) {
          HttpClientContext context = HttpClientContext.create();
          RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
          requestConfigBuilder.setSocketTimeout(routerConfig.getHttpasyncclientConnectionWarmingSocketTimeoutMs());
          context.setRequestConfig(requestConfigBuilder.build());

          Future responseFuture = client
              .execute(requestProducer, new BlockingAsyncResponseConsumer(latch, instanceUrl), context, dummyCallback);
          connectionWarmupResponseFutures.add(responseFuture);
          // To avoid overwhelming storage nodes
          Thread.sleep(connectionWarmingSleepIntervalMs);
        }
        /**
         * At this moment, all the new connection creation requests should be sent completely,
         * then we could let the response processing proceed.
         */
        latch.countDown();
        for (Future future: connectionWarmupResponseFutures) {
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

    @Override
    public void stopInner() throws Exception {
      clientConnHealthinessScannerStopped = true;
      clientConnHealthinessScannerThread.interrupt();
    }
  }

  private HttpClientUtils.ClosableHttpAsyncClientWithConnManager createAndStartNewClient() {
    HttpClientUtils.ClosableHttpAsyncClientWithConnManager clientWithConnManager =
        HttpClientUtils.getMinimalHttpClientWithConnManager(
            ioThreadNumPerClient,
            maxConnPerRoutePerClient,
            totalMaxConnPerClient,
            socketTimeout,
            connectionTimeout,
            sslFactory,
            dnsResolver,
            Optional.of(poolStats),
            routerConfig.isIdleConnectionToServerCleanupEnabled(),
            routerConfig.getIdleConnectionToServerCleanupThresholdMins());
    clientWithConnManager.getClient().start();
    return clientWithConnManager;
  }

  @Override
  public void sendRequest(VeniceMetaDataRequest request, CompletableFuture<PortableHttpResponse> responseFuture) {
    String host = request.getNodeId();
    CloseableHttpAsyncClient client = getHttpClientForHost(host);
    // during warmup up it might return null
    if (client == null) {
      responseFuture.complete(null);
      return;
    }
    HttpGet httpGet = new HttpGet(request.getUrl() + request.getQuery());

    if (request.hasTimeout()) {
      RequestConfig requestConfig = RequestConfig.custom()
          .setConnectTimeout(request.getTimeout())
          .setConnectionRequestTimeout(request.getTimeout())
          .build();
      httpGet.setConfig(requestConfig);
    }

    client.execute(
        httpGet,
        new HttpAsyncClientFutureCallBack(
            responseFuture::complete,
            responseFuture::completeExceptionally,
            () -> responseFuture.cancel(false)));
  }

  @Override
  public void query(
      Instance host,
      VenicePath path,
      Consumer<PortableHttpResponse> completedCallBack,
      Consumer<Throwable> failedCallBack,
      BooleanSupplier cancelledCallBack) throws RouterException {
    /*
     * This function call is used to populate per-storage-node stats gradually since the connection pool
     * is empty at the very beginning.
     */
    poolStats.addStatsForRoute(host.getHost());

    // http(s)://host:port/path
    String address = host.getHostUrl(sslFactory.isPresent());
    final HttpUriRequest routerRequest = path.composeRouterRequest(address);
    // set up header to pass map required by the Venice server
    path.setupVeniceHeaders((k, v) -> routerRequest.addHeader(k, v));
    CloseableHttpAsyncClient selectedClient;
    if (perNodeClientEnabled) {
      // If all the pool are used up by the set of live instances, spawn new client
      selectedClient = nodeIdToClientMap.computeIfAbsent(host.getNodeId(), h -> createAndStartNewClient()).getClient();
    } else {
      int selectedClientId = Math.abs(random.nextInt() % clientPoolSize);
      selectedClient = clientPool.get(selectedClientId);
    }

    selectedClient.execute(
        routerRequest,
        new HttpAsyncClientFutureCallBack(completedCallBack, failedCallBack, cancelledCallBack));
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
    public ByteBuf getContentInByteBuf() throws IOException {
      byte[] contentToByte;
      try (InputStream contentStream = httpResponse.getEntity().getContent()) {
        contentToByte = IOUtils.toByteArray(contentStream);
      }
      return Unpooled.wrappedBuffer(contentToByte);
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
