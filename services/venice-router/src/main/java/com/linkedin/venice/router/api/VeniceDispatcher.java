package com.linkedin.venice.router.api;

import static com.linkedin.venice.HttpConstants.VENICE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.HttpConstants.VENICE_REQUEST_RCU;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.router.api.PartitionDispatchHandler4;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.alpini.router.api.Scatter;
import com.linkedin.alpini.router.api.ScatterGatherRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.httpclient.PortableHttpResponse;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.VeniceChunkedResponse;
import com.linkedin.venice.router.throttle.PendingRequestThrottler;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public final class VeniceDispatcher implements PartitionDispatchHandler4<Instance, VenicePath, RouterKey> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceDispatcher.class);
  /**
   * Prefix for slow multiget request log throttling identifier.
   * Combined with store name to create unique throttling keys per store.
   */
  private static final String SLOW_MULTIGET_REQUEST_LOG_PREFIX = "SLOW_MULTIGET_REQUEST_";
  /**
   * Singleton filter for throttling redundant slow request logs to prevent log spamming.
   */
  private static final RedundantExceptionFilter REDUNDANT_LOG_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  /**
   * This map is used to capture all the {@link CompletableFuture} returned by {@link #storageNodeClient},
   * and it is used to clean up the leaked futures in {@link LeakedCompletableFutureCleanupService}.
   */
  private final VeniceConcurrentHashMap<Long, TimedCompletableFuture> responseFutureMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, ReentrantLock> storageNodeLockMap = new VeniceConcurrentHashMap<>();
  private final AtomicLong uniqueRequestId = new AtomicLong(0);

  private static final Set<Integer> PASS_THROUGH_ERROR_CODES = Utils.setOf(TOO_MANY_REQUESTS.code());
  private static final Set<Integer> RETRIABLE_ERROR_CODES =
      Utils.setOf(INTERNAL_SERVER_ERROR.code(), SERVICE_UNAVAILABLE.code());

  private final VeniceRouterConfig routerConfig;
  private final ReadOnlyStoreRepository storeRepository;

  private final StorageNodeClient storageNodeClient;
  private final PendingRequestThrottler pendingRequestThrottler;

  private final RouteHttpRequestStats routeHttpRequestStats;
  private final RouterStats<RouteHttpStats> perRouteStatsByType;
  private final RouterStats<AggRouterHttpRequestStats> perStoreStatsByType;

  private final AggHostHealthStats aggHostHealthStats;
  private final long routerUnhealthyPendingConnThresholdPerRoute;
  private final long slowScatterRequestThresholdMs;

  private final boolean isStatefulHealthCheckEnabled;

  private final LeakedCompletableFutureCleanupService leakedCompletableFutureCleanupService;

  private final RouterStats<AggRouterHttpRequestStats> routerStats;

  public VeniceDispatcher(
      VeniceRouterConfig config,
      ReadOnlyStoreRepository storeRepository,
      RouterStats<AggRouterHttpRequestStats> perStoreStatsByType,
      MetricsRepository metricsRepository,
      StorageNodeClient storageNodeClient,
      RouteHttpRequestStats routeHttpRequestStats,
      AggHostHealthStats aggHostHealthStats,
      RouterStats<AggRouterHttpRequestStats> routerStats) {
    this.routerConfig = config;
    this.routerUnhealthyPendingConnThresholdPerRoute = routerConfig.getRouterUnhealthyPendingConnThresholdPerRoute();
    this.slowScatterRequestThresholdMs = routerConfig.getSlowScatterRequestThresholdMs();
    this.isStatefulHealthCheckEnabled = routerConfig.isStatefulRouterHealthCheckEnabled();
    this.storeRepository = storeRepository;
    this.routeHttpRequestStats = routeHttpRequestStats;
    this.perRouteStatsByType = new RouterStats<>(requestType -> new RouteHttpStats(metricsRepository, requestType));
    this.perStoreStatsByType = perStoreStatsByType;
    this.storageNodeClient = storageNodeClient;
    this.pendingRequestThrottler = new PendingRequestThrottler(config.getMaxPendingRequest());
    this.aggHostHealthStats = aggHostHealthStats;

    this.leakedCompletableFutureCleanupService = new LeakedCompletableFutureCleanupService();
    this.leakedCompletableFutureCleanupService.start();
    this.routerStats = routerStats;
  }

  public RouterStats<RouteHttpStats> getPerRouteStatsByType() {
    return perRouteStatsByType;
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
      @Nonnull Executor executor) throws RouterException {
    String storeName = path.getStoreName();
    RequestType requestType = path.getRequestType();
    path.recordOriginalRequestStartTimestamp();

    if (part.getHosts().size() != 1) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          storeName,
          requestType,
          INTERNAL_SERVER_ERROR,
          "There should be only one chosen replica for the request: " + part);
    }

    Instance storageNode = part.getHosts().get(0);
    hostSelected.setSuccess(storageNode);

    // Track dispatch start time for slow request logging
    long dispatchStartTimeNs = System.nanoTime();

    // sendRequest completes future either immediately in the calling thread context or on the executor
    sendRequest(storageNode, path, retryFuture).whenComplete((response, throwable) -> {
      try {
        // Log slow scatter requests to help debug high P99 latency (with throttling to prevent log spamming)
        double elapsedTimeMs = LatencyUtils.getElapsedTimeFromNSToMS(dispatchStartTimeNs);
        if (elapsedTimeMs > slowScatterRequestThresholdMs && RequestType.isStreaming(requestType)) {
          String throttleKey = SLOW_MULTIGET_REQUEST_LOG_PREFIX + storeName;
          if (!REDUNDANT_LOG_FILTER.isRedundantException(throttleKey)) {
            logSlowScatterRequest(path, part, storageNode, elapsedTimeMs, response, throwable);
          }
        }

        int statusCode = response != null ? response.getStatusCode() : HttpStatus.SC_INTERNAL_SERVER_ERROR;
        if (!retryFuture.isCancelled() && RETRIABLE_ERROR_CODES.contains(statusCode)) {
          retryFuture.setSuccess(HttpResponseStatus.valueOf(statusCode));
          AggRouterHttpRequestStats stats = routerStats.getStatsByType(requestType);
          stats.recordErrorRetryCount(storeName);
          return;
        }

        if (throwable != null) {
          throw throwable;
        }

        // Do not mark storage node fast for 429 status code
        if (statusCode < HttpStatus.SC_INTERNAL_SERVER_ERROR && statusCode != HttpStatus.SC_TOO_MANY_REQUESTS) {
          path.markStorageNodeAsFast(storageNode.getNodeId());
        }

        responseFuture.setSuccess(Collections.singletonList(buildResponse(path, response)));
      } catch (Throwable e) {
        responseFuture.setFailure(e);
      }
    });
  }

  protected CompletableFuture<PortableHttpResponse> sendRequest(
      Instance storageNode,
      VenicePath path,
      AsyncPromise<HttpResponseStatus> retryFuture) throws RouterException {
    String storeName = path.getStoreName();
    String hostName = storageNode.getHost();
    RequestType requestType = path.getRequestType();

    long startTime = System.nanoTime();
    TimedCompletableFuture<PortableHttpResponse> responseFuture =
        new TimedCompletableFuture<>(System.currentTimeMillis(), storageNode.getNodeId());

    /**
     * TODO: Consider removing the per router level pendingRequestThrottler once {@link RouterThrottleHandler} is
     *       enabled; {@link RouterThrottleHandler} is also a per router level pending request throttler but works
     *       in the earlier stack before scattering any request, which can save more resource.
     */
    if (!pendingRequestThrottler.put()) {
      AggRouterHttpRequestStats stats = routerStats.getStatsByType(requestType);
      stats.recordRequestThrottledByRouterCapacity(storeName);
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          storeName,
          requestType,
          SERVICE_UNAVAILABLE,
          "Maximum number of pending request threshold reached! Current pending request count: "
              + pendingRequestThrottler.getCurrentPendingRequestCount());
    }

    ReentrantLock lock = storageNodeLockMap.computeIfAbsent(hostName, id -> new ReentrantLock());
    boolean isRequestThrottled = false;
    lock.lock();
    try {
      long pendingRequestCount = routeHttpRequestStats.getPendingRequestCount(storageNode.getNodeId());

      if (isStatefulHealthCheckEnabled && pendingRequestCount > routerUnhealthyPendingConnThresholdPerRoute) {
        isRequestThrottled = true;
        // try to trigger error retry if its not cancelled already. if retry is cancelled throw exception which
        // increases the unhealthy request metric.
        if (!retryFuture.isCancelled()) {
          retryFuture.setSuccess(INTERNAL_SERVER_ERROR);
          responseFuture.completeExceptionally(
              new VeniceException("Triggering error retry, too many pending request to storage node :" + hostName));
          perStoreStatsByType.getStatsByType(path.getRequestType())
              .recordErrorRetryAttemptTriggeredByPendingRequestCheck(storeName);
          return responseFuture;
        } else {
          throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
              storeName,
              requestType,
              SERVICE_UNAVAILABLE,
              "Too many pending request to storage node : " + hostName);
        }
      }
      routeHttpRequestStats.recordPendingRequest(storageNode.getNodeId());

      long requestId = uniqueRequestId.getAndIncrement();
      responseFutureMap.put(requestId, responseFuture);
      try {
        /**
         * Mark that the storage node will be used by current request and this piece of information will be used
         * to decide whether a storage node is suitable for retry request.
         */
        path.requestStorageNode(storageNode.getNodeId());
        storageNodeClient.query(
            storageNode,
            path,
            responseFuture::complete,
            responseFuture::completeExceptionally,
            () -> responseFuture.cancel(false));
      } catch (Throwable throwable) {
        responseFuture.completeExceptionally(throwable);
      }
      return responseFuture.whenComplete((response, throwable) -> {
        RouteHttpStats perRouteStats = perRouteStatsByType.getStatsByType(requestType);
        perRouteStats
            .recordResponseWaitingTime(storageNode.getHost(), LatencyUtils.getElapsedTimeFromNSToMS(startTime));
        routeHttpRequestStats.recordFinishedRequest(storageNode.getNodeId());
        pendingRequestThrottler.take();
        responseFutureMap.remove(requestId);
      });
    } finally {
      if (isRequestThrottled) {
        pendingRequestThrottler.take();
      }
      lock.unlock();
    }
  }

  protected VeniceFullHttpResponse buildResponse(VenicePath path, PortableHttpResponse serverResponse)
      throws IOException {
    int statusCode = serverResponse.getStatusCode();
    ByteBuf content = serverResponse.getContentInByteBuf();

    if (PASS_THROUGH_ERROR_CODES.contains(statusCode)) {
      return buildPlainTextResponse(HttpResponseStatus.valueOf(statusCode), content);
    }

    CompressionStrategy contentCompression =
        VeniceResponseDecompressor.getCompressionStrategy(serverResponse.getFirstHeader(VENICE_COMPRESSION_STRATEGY));

    long decompressionTimeInNs = 0;

    if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_NOT_FOUND) {
      statusCode = HttpStatus.SC_BAD_GATEWAY;
    }

    if (statusCode == HttpStatus.SC_OK) {
      VeniceResponseDecompressor responseDecompressor = path.getResponseDecompressor();
      if (path.isStreamingRequest()) {
        VeniceChunkedResponse chunkedResponse = path.getChunkedResponse();
        if (path.getRequestType().equals(RequestType.MULTI_GET_STREAMING)) {
          Pair<ByteBuf, CompressionStrategy> chunk =
              responseDecompressor.processMultiGetResponseForStreaming(contentCompression, content);
          chunkedResponse.write(chunk.getFirst(), chunk.getSecond());
        } else {
          chunkedResponse.write(content);
        }
        content = Unpooled.EMPTY_BUFFER;
      } else {
        final ContentDecompressResult contentDecompressResult;
        switch (path.getRequestType()) {
          case SINGLE_GET:
            contentDecompressResult = responseDecompressor.decompressSingleGetContent(contentCompression, content);
            break;
          case MULTI_GET:
            contentDecompressResult = responseDecompressor.decompressMultiGetContent(contentCompression, content);
            break;
          case COMPUTE:
            // Compute requests are decompressed on the SN
            contentDecompressResult = new ContentDecompressResult(content, CompressionStrategy.NO_OP, 0);
            break;
          default:
            throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
                null,
                null,
                INTERNAL_SERVER_ERROR,
                "Unknown request type: " + path.getRequestType());
        }

        content = contentDecompressResult.getContent();
        contentCompression = contentDecompressResult.getCompressionStrategy();
        decompressionTimeInNs = contentDecompressResult.getDecompressionTimeInNs();
      }
    } else {
      contentCompression = CompressionStrategy.NO_OP;
    }

    VeniceFullHttpResponse response = new VeniceFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.valueOf(statusCode),
        content,
        decompressionTimeInNs);
    response.headers()
        .set(HttpHeaderNames.CONTENT_TYPE, serverResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE))
        .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
        .set(HttpConstants.VENICE_SCHEMA_ID, serverResponse.getFirstHeader(HttpConstants.VENICE_SCHEMA_ID))
        .set(HttpConstants.VENICE_COMPRESSION_STRATEGY, contentCompression.getValue())
        .set(
            VENICE_REQUEST_RCU,
            serverResponse.containsHeader(VENICE_REQUEST_RCU) ? serverResponse.getFirstHeader(VENICE_REQUEST_RCU) : 1);
    return response;
  }

  /**
   * For TEST ONLY
   */
  public RouteHttpRequestStats getRouteHttpRequestStats() {
    return routeHttpRequestStats;
  }

  /**
   * For TEST ONLY
   */
  public PendingRequestThrottler getPendingRequestThrottler() {
    return pendingRequestThrottler;
  }

  protected VeniceFullHttpResponse buildPlainTextResponse(HttpResponseStatus status, ByteBuf content) {
    VeniceFullHttpResponse response = new VeniceFullHttpResponse(HttpVersion.HTTP_1_1, status, content, 0L);
    response.headers()
        .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.TEXT_PLAIN)
        .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    return response;
  }

  /**
   * Logs detailed information about slow scatter requests to help debug high P99 latency issues.
   * This method is called when a scatter request takes longer than the configured threshold.
   *
   * @param path the Venice path containing store and version information
   * @param part the scatter gather request part containing partition keys
   * @param storageNode the storage node that the request was sent to
   * @param elapsedTimeMs the time taken for the request in milliseconds
   * @param response the response from the storage node (may be null if there was an error)
   * @param throwable any exception that occurred during the request (may be null)
   */
  private void logSlowScatterRequest(
      VenicePath path,
      ScatterGatherRequest<Instance, RouterKey> part,
      Instance storageNode,
      double elapsedTimeMs,
      PortableHttpResponse response,
      Throwable throwable) {
    try {
      String storeName = path.getStoreName();
      int version = path.getVersionNumber();
      boolean isRetry = path.isRetryRequest();
      String serverNodeId = storageNode.getNodeId();

      // Collect unique partition IDs from the scatter request
      Set<Integer> partitionIds = part.getPartitionKeys()
          .stream()
          .filter(RouterKey::hasPartitionId)
          .map(RouterKey::getPartitionId)
          .collect(Collectors.toSet());

      int statusCode = response != null ? response.getStatusCode() : -1;
      String errorMessage = throwable != null ? throwable.getMessage() : "none";

      LOGGER.warn(
          "Slow scatter request detected - store: {}, version: {}, partitions: {}, "
              + "isRetryRequest: {}, serverNode: {}, elapsedTimeMs: {}, statusCode: {}, error: {}",
          storeName,
          version,
          partitionIds,
          isRetry,
          serverNodeId,
          String.format("%.2f", elapsedTimeMs),
          statusCode,
          errorMessage);
    } catch (Exception e) {
      // Don't let logging errors affect request processing
      LOGGER.error("Failed to log slow scatter request", e);
    }
  }

  public void stop() {
    this.leakedCompletableFutureCleanupService.interrupt();
  }

  /**
   * This implementation of {@link CompletableFuture} has the capability to track the start time.
   * @param <T>
   */
  private static class TimedCompletableFuture<T> extends CompletableFuture<T> {
    private final long requestTime;
    private final String hostName;

    public TimedCompletableFuture(long requestTime, String hostName) {
      this.requestTime = requestTime;
      this.hostName = hostName;
    }

    public String toString() {
      return getClass().getName() + " for request to " + hostName + " at timestamp: " + requestTime;
    }
  }

  /**
   * This class is used to clean up leaked CompletableFuture returned by {@link #storageNodeClient}.
   * We noticed that this leaking behavior is happening to HttpAsyncClient, which means the callback of
   * returned future will never be triggered, which makes the stateful health check problematic.
   * Since we still don't know the root cause in HttpAsyncClient, this service will try to mitigate the impact.
   */
  private class LeakedCompletableFutureCleanupService extends Thread {
    private final long pollIntervalMs;
    private final long cleanupThresholdMs;

    public LeakedCompletableFutureCleanupService() {
      super("LeakedCompletableFutureCleanupService");
      this.pollIntervalMs = routerConfig.getLeakedFutureCleanupPollIntervalMs();
      this.cleanupThresholdMs = routerConfig.getLeakedFutureCleanupThresholdMs();
    }

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(pollIntervalMs);
          responseFutureMap.forEach((requestId, responseFuture) -> {
            if (System.currentTimeMillis() - responseFuture.requestTime >= cleanupThresholdMs) {
              LOGGER.warn("Cleaning up the leaked response future: {}", responseFuture);
              responseFuture.completeExceptionally(new VeniceException("Leaking response future"));
              aggHostHealthStats.recordLeakedPendingRequestCount(responseFuture.hostName);
            }
          });
        } catch (InterruptedException e) {
          LOGGER.info("LeakedCompletableFutureCleanupService was interrupt, will exit", e);
          break;
        }
      }
    }
  }
}
