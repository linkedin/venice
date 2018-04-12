package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.AsyncPromise;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.PartitionDispatchHandler4;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.common.PartitionOffsetMapUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.api.path.VeniceSingleGetPath;
import com.linkedin.venice.router.cache.RouterCache;
import com.linkedin.venice.router.httpclient.CachedDnsResolver;
import com.linkedin.venice.router.httpclient.HttpClientUtils;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.HttpConnectionPoolStats;
import com.linkedin.venice.router.stats.RouteHttpStats;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.log4j.Logger;

import static com.linkedin.venice.HttpConstants.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


public class VeniceDispatcher implements PartitionDispatchHandler4<Instance, VenicePath, RouterKey>, Closeable{
  private final String scheme;

  private static final Logger logger = Logger.getLogger(VeniceDispatcher.class);

  // see: https://hc.apache.org/httpcomponents-asyncclient-dev/quickstart.html
  private final int clientPoolSize;
  private final ArrayList<CloseableHttpAsyncClient> clientPool;
  private final Random random = new Random();

  // key is (resource + "_" + partition)
  private final ConcurrentMap<String, Long> offsets = new ConcurrentHashMap<>();
  private final VeniceHostHealth healthMonitor;

   // How many offsets behind can a storage node be for a partition and still be considered 'caught up'
  private long acceptableOffsetLag = 10000; /* TODO: make this configurable for streaming use-case */

  private final ReadOnlyStoreRepository storeRepository;
  private final Optional<RouterCache> routerCache;
  private final double cacheHitRequestThrottleWeight;
  private static final ByteBuf NOT_FOUND_CONTENT = Unpooled.wrappedBuffer(new byte[0]);

  private final AggRouterHttpRequestStats statsForSingleGet;

  /**
   * Single-get throttling needs to happen here because of caching.
   */
  private ReadRequestThrottler readRequestThrottler;

  private final HttpConnectionPoolStats poolStats;

  private final RouteHttpStats routeStatsForSingleGet;
  private final RouteHttpStats routeStatsForMultiGet;


  /**
   *
   * @param healthMonitor
   * @param sslFactory if this is present, it will be used to make SSL requests to storage nodes.
   */
  public VeniceDispatcher(VeniceRouterConfig config, VeniceHostHealth healthMonitor,
      Optional<SSLEngineComponentFactory> sslFactory, ReadOnlyStoreRepository storeRepository,
      Optional<RouterCache> routerCache, AggRouterHttpRequestStats statsForSingleGet,
      Optional<CachedDnsResolver> dnsResolver, MetricsRepository metricsRepository) {
    this.healthMonitor = healthMonitor;
    this.scheme = sslFactory.isPresent() ? HTTPS_PREFIX : HTTP_PREFIX;
    this.storeRepository = storeRepository;
    this.routerCache = routerCache;
    this.cacheHitRequestThrottleWeight = config.getCacheHitRequestThrottleWeight();
    this.statsForSingleGet = statsForSingleGet;

    this.clientPoolSize = config.getHttpClientPoolSize();
    int totalIOThreadNum = Runtime.getRuntime().availableProcessors();
    int maxConnPerRoute = config.getMaxOutgoingConnPerRoute();
    int maxConn = config.getMaxOutgoingConn();

    this.poolStats = new HttpConnectionPoolStats(metricsRepository, "connection_pool");
    this.routeStatsForSingleGet = new RouteHttpStats(metricsRepository, RequestType.SINGLE_GET);
    this.routeStatsForMultiGet = new RouteHttpStats(metricsRepository, RequestType.MULTI_GET);

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
          totalMaxConnPerClient, sslFactory, dnsResolver, Optional.of(poolStats));
      client.start();
      clientPool.add(client);
    }
  }

  public void initReadRequestThrottler(ReadRequestThrottler requestThrottler) {
    if (null != this.readRequestThrottler) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.empty(), Optional.empty(), INTERNAL_SERVER_ERROR,
          "ReadRequestThrottle has already been initialized before, and no further update expected!");
    }
    this.readRequestThrottler = requestThrottler;
  }

  @Override
  public void dispatch(
      @Nonnull Scatter<Instance, VenicePath, RouterKey> scatter,
      @Nonnull ScatterGatherRequest<Instance, RouterKey> part,
      @Nonnull VenicePath path,
      @Nonnull BasicHttpRequest request,
      @Nonnull AsyncPromise<Instance> hostSelected,
      @Nonnull AsyncPromise<List<FullHttpResponse>> responseFuture,
      @Nonnull AsyncPromise<HttpResponseStatus> retryFuture,
      @Nonnull AsyncFuture<Void> timeoutFuture,
      @Nonnull Executor contextExecutor) throws RouterException {
    long dispatchStartTSInNS = System.nanoTime();
    if (null == readRequestThrottler) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.empty(), Optional.empty(), INTERNAL_SERVER_ERROR,
          "Read request throttle has not been setup yet");
    }

    String storeName = path.getStoreName();
    Instance host;
    try {
      int hostCount = part.getHosts().size();
      if (1 != hostCount) {
        throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(path.getRequestType()),
            INTERNAL_SERVER_ERROR, "There should be only one chosen replica for request: " + part);
      }
      host = part.getHosts().get(0);
      hostSelected.setSuccess(host);
    } catch (Exception e) {
      hostSelected.setFailure(e);
      throw e;
    }
    /**
     * This function call is used to populate per-storage-node stats gradually since the connection pool
     * is empty at the very beginning.
     */
    poolStats.addStatsForRoute(host.getHost());

    if (path.getRequestType().equals(RequestType.SINGLE_GET) &&
        handleCacheLookupAndThrottlingForSingleGetRequest((VeniceSingleGetPath)path, host, responseFuture, contextExecutor)) {
      // Cache hit
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Routing request to host: " + host.getHost() + ":" + host.getPort());
    }

    //  http(s)://host:port/path
    String address = scheme + host.getHost() + ":" + host.getPort() + "/";
    final HttpUriRequest routerRequest = path.composeRouterRequest(address);

    CloseableHttpAsyncClient selectedClient = clientPool.get(Math.abs(random.nextInt() % clientPoolSize));
    selectedClient.execute(routerRequest, new FutureCallback<org.apache.http.HttpResponse>() {

      @Override
      public void completed(org.apache.http.HttpResponse result) {
        int statusCode = result.getStatusLine().getStatusCode();
        Set<String> partitionNames = part.getPartitionsNames();
        String resourceName = path.getResourceName();
        String offsetHeader = result.getFirstHeader(HttpConstants.VENICE_OFFSET).getValue();
        // TODO: make this logic consistent across single-get and multi-get
        switch (path.getRequestType()) {
          case SINGLE_GET:
            Iterator<String> partitionIterator = partitionNames.iterator();
            String partitionName = partitionIterator.next();
            if (partitionIterator.hasNext()) {
              logger.error(
                  "There must be only one partition in a request, handling request as if there is only one partition");
            }
            long offset = Long.parseLong(offsetHeader);
            if (statusCode == HttpStatus.SC_OK) {
              checkOffsetLag(resourceName, partitionName, host, offset);
              /*
              // The following code could block online serving if all the partitions are marked as slow.
              // And right now there is no logic to randomly return one host if none is available;
              // TODO: find a way to mark host slow safely.

              healthMonitor.setPartitionAsSlow(host, partitionName);
              contextExecutor.execute(() -> {
                // Triggers an immediate router retry excluding the host we selected.
                retryFuture.setSuccess(HttpResponseStatus.SERVICE_UNAVAILABLE);
              });
              return;
              */
            }
            break;
          case MULTI_GET:
            // Get partition offset header
            try {
              Map<Integer, Long> partitionOffsetMap = PartitionOffsetMapUtils.deserializePartitionOffsetMap(offsetHeader);
              partitionNames.forEach(pName -> {
                int partitionId = HelixUtils.getPartitionId(pName);
                if (partitionOffsetMap.containsKey(partitionId)) {
                  /**
                   * TODO: whether we should mark host as slow for multi-get request.
                   *
                   * Right now, the scatter mode being used for multi-get only returns one host per request, so we
                   * could not mark it slow directly if the offset lag is big since it could potentially mark all the hosts
                   * to be slow.
                   *
                   * For streaming case, one possible solution is to use sticky routing so that the requests for a given
                   * partition will hit one specific host consistently.
                   */
                  checkOffsetLag(resourceName, pName, host, partitionOffsetMap.get(partitionId));

                } else {
                  logger.error("Multi-get response doesn't contain offset for partition: " + pName);
                }
              });
            } catch (IOException e) {
              logger.error("Failed to parse partition offset map from content: " + offsetHeader);
            }
            break;
        }
        int responseStatus = result.getStatusLine().getStatusCode();
        FullHttpResponse response;
        byte[] contentToByte;

        try (InputStream contentStream = result.getEntity().getContent()) {
          contentToByte = IOUtils.toByteArray(contentStream);
        } catch (IOException e) {
          completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
          return;
        }

        ByteBuf content = Unpooled.wrappedBuffer(contentToByte);

        switch (responseStatus){
          case HttpStatus.SC_OK:
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
            break;
          case HttpStatus.SC_NOT_FOUND:
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, content);
            break;
          case HttpStatus.SC_INTERNAL_SERVER_ERROR:
          default: //Path Parser will throw BAD_REQUEST responses.
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, BAD_GATEWAY, content);
        }

        int valueSchemaId = Integer.parseInt(result.getFirstHeader(HttpConstants.VENICE_SCHEMA_ID).getValue());
        CompressionStrategy compressionStrategy = result.containsHeader(VENICE_COMPRESSION_STRATEGY)
            ? CompressionStrategy.valueOf(Integer.valueOf(result.getFirstHeader(VENICE_COMPRESSION_STRATEGY).getValue()))
            : CompressionStrategy.NO_OP;
        response.headers()
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
            .set(HttpHeaderNames.CONTENT_TYPE, result.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue())
            .set(HttpConstants.VENICE_SCHEMA_ID, valueSchemaId)
            .set(HttpConstants.VENICE_COMPRESSION_STRATEGY, compressionStrategy.getValue());
        if (path.getRequestType().equals(RequestType.SINGLE_GET)) {
          // For multi-get, the partition is not returned to client
          String partitionIdStr = numberFromPartitionName(partitionNames.iterator().next());
          response.headers().set(HttpConstants.VENICE_PARTITION, partitionIdStr);
          // Update cache for single-get request
          if (responseStatus == HttpStatus.SC_OK) {
            updateCacheForSingleGetRequest((VeniceSingleGetPath) path, Optional.of(contentToByte), Optional.of(valueSchemaId), compressionStrategy);
          } else if (responseStatus == HttpStatus.SC_NOT_FOUND) {
            updateCacheForSingleGetRequest((VeniceSingleGetPath) path, Optional.empty(), Optional.empty(), compressionStrategy);
          }
        }

        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });
        recordResponseWaitingTime(host.getHost(), path, dispatchStartTSInNS);
      }

      @Override
      public void failed(Exception ex) {
        completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
        recordResponseWaitingTime(host.getHost(), path, dispatchStartTSInNS);
      }

      @Override
      public void cancelled() {
        completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            new VeniceException("Request to storage node was cancelled"));
        recordResponseWaitingTime(host.getHost(), path, dispatchStartTSInNS);
      }

      private void completeWithError(HttpResponseStatus status, Throwable e) {

        String errMsg = e.getMessage();
        if (null == errMsg) {
          errMsg = "Unknown error, caught: " + e.getClass().getCanonicalName();
        }
        ByteBuf content =  Unpooled.wrappedBuffer(errMsg.getBytes(StandardCharsets.UTF_8));

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.TEXT_PLAIN)
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });
      }
    });
  }

  private void recordResponseWaitingTime(String hostName, VenicePath path, long dispatchStartTSInNS) {
    double latencyInMS = LatencyUtils.getLatencyInMS(dispatchStartTSInNS);
    if (path.getRequestType().equals(RequestType.SINGLE_GET)) {
      routeStatsForSingleGet.recordResponseWaitingTime(hostName, latencyInMS);
    } else {
      routeStatsForMultiGet.recordResponseWaitingTime(hostName, latencyInMS);
    }
  }

  /**
   * Handle cache lookup for single-get request, and this function is handling throttling as well.
   * Here is the throttling logic for cache lookup request:
   * 1. If it is a cache hit, the request won't be counted when calculating per storage node throttler
   * since there is no request sent out to any storage node;
   * 2. If it is a cache miss or cache is not enabled, the request will be counted when calculating store throttler
   * and per storage node throttler as before;
   *
   * Only apply quota enforcement for regular request, but not retry request.
   * The reason is that retry is a way for latency guarantee, which should be transparent to customers.
   *
   * @param path
   * @param selectedHost
   * @param responseFuture
   * @param contextExecutor
   * @return whether cache lookup is succeed or not.
   */
  protected boolean handleCacheLookupAndThrottlingForSingleGetRequest(VeniceSingleGetPath path, Instance selectedHost,
      AsyncPromise<List<FullHttpResponse>> responseFuture, Executor contextExecutor) throws RouterException {
    String storeName = path.getStoreName();
    try {
      if (routerCache.isPresent() && storeRepository.isRouterCacheEnabled(storeName)) {
        /**
         * Cache throttling first
         * Only throttle in store level since the cache lookup request is not actually sending to any storage node
         */
        if (!path.isRetryRequest()) {
          readRequestThrottler.mayThrottleRead(storeName, cacheHitRequestThrottleWeight * readRequestThrottler.getReadCapacity(), Optional.empty());
        }

        long startTimeInNS = System.nanoTime();
        statsForSingleGet.recordCacheLookupRequest(storeName);
        Optional<RouterCache.CacheValue> cacheValue =
            routerCache.get().get(storeName, path.getVersionNumber(), path.getPartitionKey().getBytes());
        statsForSingleGet.recordCacheLookupLatency(storeName, LatencyUtils.getLatencyInMS(startTimeInNS));
        if (cacheValue != null) {
          // Cache hit
          statsForSingleGet.recordCacheHitRequest(storeName);
          FullHttpResponse response;
          if (cacheValue.isPresent()) {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(cacheValue.get().getValue()));
            response.headers()
                .set(HttpConstants.VENICE_PARTITION, path.getPartition())
                .set(HttpConstants.VENICE_SCHEMA_ID, cacheValue.get().getSchemaId())
                .set(HttpHeaderNames.CONTENT_LENGTH, cacheValue.get().getValue().length)
                .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);
          } else {
            response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, NOT_FOUND_CONTENT);
            response.headers()
                .set(HttpHeaderNames.CONTENT_LENGTH, 0)
                .set(HttpConstants.VENICE_PARTITION, path.getPartition());
          }

          response.headers().set(VENICE_COMPRESSION_STRATEGY, routerCache.get().getCompressionStrategy(path.getResourceName()).getValue());

          contextExecutor.execute(() -> {
            responseFuture.setSuccess(Collections.singletonList(response));
          });
          return true;
        } else {
          /**
           * Cache miss
           * Unset the previous per-store throttler
           */
          if (!path.isRetryRequest()) {
            readRequestThrottler.mayThrottleRead(storeName, -cacheHitRequestThrottleWeight * readRequestThrottler.getReadCapacity(), Optional.empty());
          }
        }
      }
      // Caching is not enabled or cache miss
      if (!path.isRetryRequest()) {
        readRequestThrottler.mayThrottleRead(storeName, readRequestThrottler.getReadCapacity(), Optional.of(selectedHost.getNodeId()));
      }
    } catch (QuotaExceededException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(RequestType.SINGLE_GET),
          TOO_MANY_REQUESTS, "Quota exceeds! msg: " + e.getMessage());
    }

    return false;
  }

  /**
   * Update cache for single-get request
   * @param path
   * @param content If not found, this field will be {@link Optional#empty()}
   * @param valueSchemaId If not found, this field will be {@link Optional#empty()}
   */
  protected void updateCacheForSingleGetRequest(VeniceSingleGetPath path, Optional<byte[]> content,
                                                Optional<Integer> valueSchemaId, CompressionStrategy compressionStrategy) {
    String storeName = path.getStoreName();
    // Setup cache for single-get
    if (routerCache.isPresent() && storeRepository.isRouterCacheEnabled(storeName)) {
      long startTimeInNS = System.nanoTime();
      statsForSingleGet.recordCachePutRequest(storeName);
      try {
        if (content.isPresent() && valueSchemaId.isPresent()) {
          RouterCache.CacheValue cacheValue = new RouterCache.CacheValue(content.get(), valueSchemaId.get());
          routerCache.get().put(storeName, path.getVersionNumber(), path.getPartitionKey().getBytes(), Optional.of(cacheValue));
        } else {
          routerCache.get().put(storeName, path.getVersionNumber(), path.getPartitionKey().getBytes(),
              Optional.empty());
        }
        routerCache.get().setCompressionType(path.getResourceName(), compressionStrategy);
      } catch (Exception e) {
        logger.error("Received exception during updating cache", e);
      }
      statsForSingleGet.recordCachePutLatency(storeName, LatencyUtils.getLatencyInMS(startTimeInNS));
    }
  }

  private String getOffsetKey(String resourceName, String partitionName) {
    return resourceName + "_" + partitionName;
  }

  public void checkOffsetLag(String resourceName, String partitionName, Instance host, long offset) {
    String offsetKey = getOffsetKey(resourceName, partitionName);
    if (offsets.containsKey(offsetKey)) {
      long prevOffset = offsets.get(offsetKey);
      long diff = prevOffset - offset;
      if (diff > acceptableOffsetLag) {
        // TODO: we should find a better way to mark host as unhealthy and still maintain high availability
        // TODO: this piece of log could impact the router performance if it gets printed log file every time
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Host: " + host + ", partition: " + partitionName + " is slower than other replica, offset diff: " + diff
                  + ", and acceptable lag: " + acceptableOffsetLag);
        }
      }
      if (diff < 0) {
        offsets.put(offsetKey, offset);
      }
    } else {
      offsets.put(offsetKey, offset);
    }
  }

  public void close(){
    clientPool.stream().forEach( client -> IOUtils.closeQuietly(client));
  }

  protected static String numberFromPartitionName(String partitionName){
    return partitionName.substring(partitionName.lastIndexOf("_")+1);
  }
}
