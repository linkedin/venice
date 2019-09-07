package com.linkedin.venice.router.httpclient;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.DnsLookupStats;
import com.linkedin.venice.router.stats.HttpConnectionPoolStats;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.log4j.Logger;

import static com.linkedin.venice.HttpConstants.*;


public class ApacheHttpAsyncStorageNodeClient implements StorageNodeClient  {
  private static final Logger logger = Logger.getLogger(ApacheHttpAsyncStorageNodeClient.class);

  private final String scheme;

  // see: https://hc.apache.org/httpcomponents-asyncclient-dev/quickstart.html
  private final int clientPoolSize;
  private final ArrayList<CloseableHttpAsyncClient> clientPool;
  private final Random random = new Random();
  private final VeniceConcurrentHashMap<String, CloseableHttpAsyncClient> hostToClientMap = new VeniceConcurrentHashMap<>();
  private final HttpConnectionPoolStats poolStats;
  private final LiveInstanceMonitor liveInstanceMonitor;

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

    if (perNodeClientEnabled) {
      ioThreadNumPerClient = config.getPerNodeClientThreadCount();
      maxConnPerRoutePerClient = maxConnPerRoute; // Per host client gets the max config per client
      totalMaxConnPerClient = maxConnPerRoute; // Using the same maxConnPerRoute, may need to tune later.
      // TODO: clean up clients when host dies.
      liveInstanceMonitor.getAllLiveInstances()
          .forEach(host -> hostToClientMap.put(host.getHost(), createAndStartNewClient()));
    } else {
      ioThreadNumPerClient = (int)Math.ceil(((double)totalIOThreadNum) / clientPoolSize);
      totalMaxConnPerClient = (int)Math.ceil(((double)maxConn) / clientPoolSize);
      maxConnPerRoutePerClient = (int)Math.ceil(((double)maxConnPerRoute) / clientPoolSize);

      for (int i = 0; i < clientPoolSize; ++i) {
        clientPool.add(createAndStartNewClient());
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

  private CloseableHttpAsyncClient createAndStartNewClient() {
    CloseableHttpAsyncClient client = HttpClientUtils.getMinimalHttpClient(ioThreadNumPerClient, maxConnPerRoutePerClient,
        totalMaxConnPerClient, socketTimeout, connectionTimeout, sslFactory, dnsResolver, Optional.of(poolStats));
    client.start();
    return client;
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
      return httpResponse.getFirstHeader(headerName).getValue();
    }
  }
}
