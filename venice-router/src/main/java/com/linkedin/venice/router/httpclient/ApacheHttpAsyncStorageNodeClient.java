package com.linkedin.venice.router.httpclient;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.VeniceHostHealth;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.cache.RouterCache;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.DnsLookupStats;
import com.linkedin.venice.router.stats.HttpConnectionPoolStats;
import com.linkedin.venice.router.throttle.PendingRequestThrottler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
import static io.netty.handler.codec.http.HttpResponseStatus.*;


public class ApacheHttpAsyncStorageNodeClient implements StorageNodeClient  {
  private static final Logger logger = Logger.getLogger(ApacheHttpAsyncStorageNodeClient.class);

  private final String scheme;

  // see: https://hc.apache.org/httpcomponents-asyncclient-dev/quickstart.html
  private final int clientPoolSize;
  private final ArrayList<CloseableHttpAsyncClient> clientPool;
  private final Random random = new Random();

  private final HttpConnectionPoolStats poolStats;

  public ApacheHttpAsyncStorageNodeClient(VeniceRouterConfig config, Optional<SSLEngineComponentFactory> sslFactory, MetricsRepository metricsRepository) {
    this.scheme = sslFactory.isPresent() ? HTTPS_PREFIX : HTTP_PREFIX;

    this.clientPoolSize = config.getHttpClientPoolSize();
    int totalIOThreadNum = Runtime.getRuntime().availableProcessors();
    int maxConnPerRoute = config.getMaxOutgoingConnPerRoute();
    int maxConn = config.getMaxOutgoingConn();

    this.poolStats = new HttpConnectionPoolStats(metricsRepository, "connection_pool");

    Optional<CachedDnsResolver> dnsResolver = Optional.empty();
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
    int ioThreadNumPerClient = (int)Math.ceil(((double)totalIOThreadNum) / clientPoolSize);
    int maxConnPerRoutePerClient = (int)Math.ceil(((double)maxConnPerRoute) / clientPoolSize);
    int totalMaxConnPerClient = (int)Math.ceil(((double)maxConn) / clientPoolSize);
    clientPool = new ArrayList<>();
    for (int i = 0; i < clientPoolSize; ++i) {
      CloseableHttpAsyncClient client = HttpClientUtils.getMinimalHttpClient(ioThreadNumPerClient, maxConnPerRoutePerClient,
          totalMaxConnPerClient, config.getSocketTimeout(), config.getConnectionTimeout(), sslFactory, dnsResolver, Optional.of(poolStats));
      client.start();
      clientPool.add(client);
    }
  }

  @Override
  public void close() {
    clientPool.stream().forEach( client -> IOUtils.closeQuietly(client));
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

    String storeName = path.getStoreName();
    //  http(s)://host:port/path
    String address = scheme + host.getHost() + ":" + host.getPort() + "/";
    final HttpUriRequest routerRequest = path.composeRouterRequest(address);

    int selectedClientId = Math.abs(random.nextInt() % clientPoolSize);
    CloseableHttpAsyncClient selectedClient = clientPool.get(selectedClientId);
    selectedClient.execute(routerRequest, new HttpAsyncClientFutureCallBack(completedCallBack, failedCallBack, cancelledCallBack));
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
