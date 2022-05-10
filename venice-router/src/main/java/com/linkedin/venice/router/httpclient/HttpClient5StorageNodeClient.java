package com.linkedin.venice.router.httpclient;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.httpclient5.HttpClient5Utils;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.HttpClient5ClientStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpClient5StorageNodeClient implements StorageNodeClient {
  private static final Logger LOGGER = LogManager.getLogger(HttpClient5StorageNodeClient.class);

  private final Random random = new Random();
  private List<CloseableHttpAsyncClient> clientList;
  private final VeniceRouterConfig routerConfig;
  private final SSLEngineComponentFactory sslFactory;
  // Timeout for the regular queries
  private final Timeout connectionTimeoutForQuery;
  private final Timeout connectTimeoutForQuery;

  private final LiveInstanceMonitor liveInstanceMonitor;
  private AtomicInteger heartBeatTimeoutCountSinceLastCheck = new AtomicInteger(0);
  private final ScheduledExecutorService repairService;
  private final HttpClient5ClientStats clientStats;

  public HttpClient5StorageNodeClient(Optional<SSLEngineComponentFactory> sslFactoryOptional, VeniceRouterConfig routerConfig,
      LiveInstanceMonitor liveInstanceMonitor, MetricsRepository metricsRepository) {
    sslFactoryOptional.orElseThrow(() -> new VeniceException("Param 'sslFactory' must be present while using " + this.getClass().getSimpleName()));
    this.sslFactory = sslFactoryOptional.get();
    if (!routerConfig.isRouterHTTP2ClientEnabled()) {
      throw new VeniceException("HTTP/2 needs to be enabled while using " + this.getClass().getSimpleName());
    }
    /**
     * HttpClient5 needs to use JDK11 to support HTTP/2, so this class will fail fast if the Java version is below JDK11.
     */
    if (Utils.getJavaMajorVersion() < 11) {
      throw new VeniceException("To enable HTTP/2 with " + this.getClass().getSimpleName() + ", the current process needs to use JDK11 or above");
    }

    /**
     * Currently, HttpClient5 H2 support won't work properly if some Venice Server instances crash.
     * This class will rely on the Router heart-beat to detect whether the client is stuck or not because of the crashed Venice Server instances.
     *
     * More details about the Router behavior when some Server instances crash:
     * 1. The H2 connection created by httpclient5 won't terminate on its own.
     * 2. After the Server is back, Router will continue to use the same H2 connection, which will stuck.
     *
     * The temp solution we put in this class:
     * 1. Every time Router heartbeat request times out, it will notify the client.
     * 2. There is a thread periodically scanning the collected timeout event.
     * 3. If the collected timeout event number exceeds the predefined threshold, it will re-create all the HttpClient5 based clients.
     *
     * TODO: follow up with the httpclient team to get a proper fix.
     */
    if (routerConfig.isHttpClient5RepairEnabled()) {
      if (!routerConfig.isRouterHeartBeatEnabled()) {
        throw new VeniceException("Router heartbeat needs to be enabled to make sure "
                + "http-client5 based StorageNodeClient with repair enabled will work properly");
      }
      this.repairService = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("HttpClient5_repair"));
    } else {
      this.repairService = null;
    }

    this.routerConfig = routerConfig;
    this.connectionTimeoutForQuery = Timeout.of(routerConfig.getSocketTimeout(), TimeUnit.MILLISECONDS);
    this.connectTimeoutForQuery = Timeout.of(routerConfig.getConnectionTimeout(), TimeUnit.MILLISECONDS);

    this.liveInstanceMonitor = liveInstanceMonitor;
    this.clientStats = new HttpClient5ClientStats(metricsRepository, "httpclient5_client");
    this.clientList = createClients();
  }

  private List<CloseableHttpAsyncClient> createClients() {
    int poolSize = routerConfig.getHttpClient5PoolSize();
    int totalIOThreadCount = routerConfig.getHttpClient5TotalIOThreadCount();
    int ioThreadCountPerClient = totalIOThreadCount / poolSize;
    List<CloseableHttpAsyncClient> newClientList = new ArrayList<>(poolSize);
    for (int cur = 0; cur < poolSize; ++cur) {
      newClientList.add(
          new HttpClient5Utils.HttpClient5Builder()
              .setSslContext(sslFactory.getSSLContext())
              .setHttpVersionPolicy(HttpVersionPolicy.FORCE_HTTP_2)
              .setIoThreadCount(ioThreadCountPerClient)
              .setRequestTimeOutInMilliseconds(routerConfig.getSocketTimeout())
              .setSkipCipherCheck(routerConfig.isHttpClient5SkipCipherCheck())
              .build());
    }
    LOGGER.info("Created HttpClient5StorageNodeClient with pool size: {}, total io thread count: {}",
        poolSize, totalIOThreadCount);
    return newClientList;
  }

  @Override
  public void start() {
    if (repairService != null) {
      repairService.scheduleAtFixedRate(() -> {
        int currentHeartBeatTimeoutCountSinceLastCheck = heartBeatTimeoutCountSinceLastCheck.get();
        LOGGER.info("Heart-beat timeout instance count since last check: {}", currentHeartBeatTimeoutCountSinceLastCheck);
        if (currentHeartBeatTimeoutCountSinceLastCheck >= routerConfig.getHttpClient5RepairThresholdOfHeartbeatTimeout()) {
          LOGGER.info("Start recreating the http-client5 based client list");
          final List<CloseableHttpAsyncClient> previousClientList = clientList;
          clientList = createClients();
          clientStats.recordClientRepair();
          closeClients(previousClientList);
          LOGGER.info("Done recreating the http-client5 based client list");
        } else {
          LOGGER.info("Heart-beat timeout instance count since last check: {} is below the threshold: {}," + " no action will be taken", currentHeartBeatTimeoutCountSinceLastCheck,
              routerConfig.getHttpClient5RepairThresholdOfHeartbeatTimeout());
        }
        heartBeatTimeoutCountSinceLastCheck.set(0);
      }, 1, routerConfig.getHttpClient5RepairCheckIntervalInMin(), TimeUnit.MINUTES);
      LOGGER.info("Started the client repair service for HttpClient5");
    }
  }

  private static void closeClients(List<CloseableHttpAsyncClient> clients) {
    clients.forEach(client -> client.close(CloseMode.GRACEFUL));
    clients.clear();
  }

  @Override
  public void close() {
    if (repairService != null) {
      repairService.shutdownNow();
      try {
        repairService.awaitTermination(30, TimeUnit.SECONDS);
        LOGGER.info("Shutdown the client repair service for HttpClient5");
      } catch (InterruptedException e) {
        throw new VeniceException("Failed to shutdown the client repair service for HttpClient5");
      }
    }
    closeClients(clientList);
  }

  @Override
  public void heartbeatTimeoutToInstance(Instance instance) {
    if (liveInstanceMonitor.isInstanceAlive(instance)) {
      heartBeatTimeoutCountSinceLastCheck.incrementAndGet();
      LOGGER.info("Recorded one heart-beat timeout to instance: " + instance);
    }
  }

  @Override
  public void query(Instance host, VenicePath path, Consumer<PortableHttpResponse> completedCallBack,
      Consumer<Throwable> failedCallBack, BooleanSupplier cancelledCallBack, long queryStartTimeInNS)
      throws RouterException {
    // Compose the request
    String uri = host.getHostUrl(true) + path.getLocation();
    Method method = Method.normalizedValueOf(path.getHttpMethod().name());
    SimpleRequestBuilder simpleRequestBuilder = SimpleRequestBuilder.create(method)
        .setUri(uri)
        .setRequestConfig(RequestConfig.custom()
            // Setup request-level timeout to make sure the request will return before the timeout.
            .setConnectionRequestTimeout(connectionTimeoutForQuery)
            .setConnectTimeout(connectTimeoutForQuery)
            .setResponseTimeout(connectionTimeoutForQuery)
            .build()
        );

    // Setup additional headers
    path.setupVeniceHeaders((k, v) -> simpleRequestBuilder.addHeader(k, v));
    Optional<byte[]> body = path.getBody();
    body.ifPresent(b -> simpleRequestBuilder.setBody(b, ContentType.DEFAULT_BINARY));

    getRandomClient().execute(simpleRequestBuilder.build(), new FutureCallback<SimpleHttpResponse>() {
      @Override
      public void completed(SimpleHttpResponse result) {
        completedCallBack.accept(new HttpClient5Response(result));
      }
      @Override
      public void failed(Exception ex) {
        failedCallBack.accept(ex);
      }
      @Override
      public void cancelled() {
        cancelledCallBack.getAsBoolean();
      }
    });
  }

  private CloseableHttpAsyncClient getRandomClient() {
    return clientList.get(random.nextInt(clientList.size()));
  }

  private static final class HttpClient5Response implements PortableHttpResponse {
    private final SimpleHttpResponse response;

    public HttpClient5Response(SimpleHttpResponse response) {
      this.response = response;
    }

    @Override
    public int getStatusCode() {
      return response.getCode();
    }

    @Override
    public ByteBuf getContentInByteBuf() throws IOException {
      /**
       * {@link SimpleHttpResponse#getBodyBytes()} will return null if the content length is 0.
       */
      byte[] body = response.getBodyBytes();
      return body == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(body);
    }

    @Override
    public boolean containsHeader(String headerName) {
      return response.containsHeader(headerName);
    }

    @Override
    public String getFirstHeader(String headerName) {
      Header header = response.getFirstHeader(headerName);
      return header != null ? header.getValue() : null;
    }
  }

  @Override
  public void sendRequest(VeniceMetaDataRequest request, CompletableFuture<PortableHttpResponse> responseFuture) {
    String uri = request.getUrl() + request.getQuery();
    Method method = Method.normalizedValueOf(request.getMethod());
    SimpleRequestBuilder simpleRequestBuilder = SimpleRequestBuilder.create(method)
        .setUri(uri);
    if (request.hasTimeout()) {
      simpleRequestBuilder.setRequestConfig(RequestConfig.custom()
          .setConnectionRequestTimeout(request.getTimeout(), TimeUnit.MILLISECONDS)
          .setResponseTimeout(request.getTimeout(), TimeUnit.MILLISECONDS)
          .build());
    }
    getRandomClient().execute(simpleRequestBuilder.build(), new FutureCallback<SimpleHttpResponse>() {
      @Override
      public void completed(SimpleHttpResponse result) {
        responseFuture.complete(new HttpClient5Response(result));
      }
      @Override
      public void failed(Exception ex) {
        responseFuture.completeExceptionally(ex);
      }
      @Override
      public void cancelled() {
        responseFuture.cancel(false);
      }
    });
  }
}
