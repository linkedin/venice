package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.AsyncPromise;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.PartitionDispatchHandler4;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VeniceMultiGetPath;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.api.path.VeniceSingleGetPath;
import com.linkedin.venice.router.cache.RouterCache;
import com.linkedin.venice.router.httpclient.PortableHttpResponse;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.VeniceChunkedResponse;
import com.linkedin.venice.router.throttle.PendingRequestThrottler;
import com.linkedin.venice.router.throttle.RouterThrottler;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import static com.linkedin.venice.HttpConstants.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


public class VeniceDispatcher implements PartitionDispatchHandler4<Instance, VenicePath, RouterKey> {
  private static final Logger logger = Logger.getLogger(VeniceDispatcher.class);

  private static final Set<Integer> PASS_THROUGH_ERROR_CODES = ImmutableSet.of(TOO_MANY_REQUESTS.code());
  private static final Set<Integer> RETRIABLE_ERROR_CODES = ImmutableSet.of(INTERNAL_SERVER_ERROR.code(), SERVICE_UNAVAILABLE.code());

  private final Optional<RouterCache> routerCache;
  private final double cacheHitRequestThrottleWeight;
  private final ReadOnlyStoreRepository storeRepository;

  private RouterThrottler readRequestThrottler;
  private final StorageNodeClient storageNodeClient;
  private final PendingRequestThrottler pendingRequestThrottler;

  private final RouteHttpRequestStats routerStats;
  private final RouterStats<RouteHttpStats> perRouteStatsByType;
  private final RouterStats<AggRouterHttpRequestStats> perStoreStatsByType;

  private final RecordSerializer<MultiGetResponseRecordV1> multiGetResponseRecordSerializer =
      SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);

  public VeniceDispatcher(
      VeniceRouterConfig config,
      VeniceHostHealth healthMonitor,
      ReadOnlyStoreRepository storeRepository,
      Optional<RouterCache> routerCache,
      RouterStats perStoreStatsByType,
      MetricsRepository metricsRepository,
      StorageNodeClient storageNodeClient,
      RouteHttpRequestStats routerStats) {

    this.storeRepository = storeRepository;
    this.routerCache = routerCache;
    this.cacheHitRequestThrottleWeight = config.getCacheHitRequestThrottleWeight();
    this.routerStats = routerStats;
    this.perRouteStatsByType = new RouterStats<>(requestType -> new RouteHttpStats(metricsRepository, requestType));
    this.perStoreStatsByType = perStoreStatsByType;
    this.storageNodeClient = storageNodeClient;
    this.pendingRequestThrottler = new PendingRequestThrottler(config.getMaxPendingRequest());
  }

  public void initReadRequestThrottler(RouterThrottler requestThrottler) {
    if (null != this.readRequestThrottler) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
          Optional.empty(),
          Optional.empty(),
          INTERNAL_SERVER_ERROR,
          "Read request throttler is already initialized.");
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
      @Nonnull Executor executor) throws RouterException {

    String storeName = path.getStoreName();
    RequestType requestType = path.getRequestType();
    path.recordOriginalRequestStartTimestamp();

    if (null == readRequestThrottler) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.of(storeName),
          Optional.of(requestType),
          INTERNAL_SERVER_ERROR,
          "Read request throttler is not initialized.");
    }

    if (requestType.equals(RequestType.COMPUTE) || requestType.equals(RequestType.COMPUTE_STREAMING)) {
      if (!storeRepository.isReadComputationEnabled(storeName)) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.of(storeName),
            Optional.of(requestType),
            METHOD_NOT_ALLOWED,
            "Read compute is not enabled for the store. Please contact Venice team to enable the feature.");
      }
    }

    if (1 != part.getHosts().size()) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.of(storeName),
          Optional.of(requestType),
          INTERNAL_SERVER_ERROR,
          "There should be only one chosen replica for the request: " + part);
    }

    Instance storageNode = part.getHosts().get(0);
    hostSelected.setSuccess(storageNode);

    if (handleCacheLookupAndThrottling(path, storageNode, responseFuture)) {
      // all keys were found in the cache
      return;
    }

    // sendRequest completes future either immediately in the calling thread context or on the executor
    sendRequest(storageNode, path, executor).whenComplete((response, throwable) -> {
      try {
        int statusCode = response != null ? response.getStatusCode() : HttpStatus.SC_INTERNAL_SERVER_ERROR;
        if (!retryFuture.isCancelled() && RETRIABLE_ERROR_CODES.contains(statusCode)) {
          retryFuture.setSuccess(INTERNAL_SERVER_ERROR);
          return;
        }

        if (throwable != null) {
          throw throwable;
        }

        if (statusCode < HttpStatus.SC_INTERNAL_SERVER_ERROR) {
          path.markStorageNodeAsFast(storageNode.getNodeId());
        }

        byte[] content = response.getContentInBytes();
        updateCache(path, response, content);
        responseFuture.setSuccess(Collections.singletonList(buildResponse(path, response, content)));

      } catch (Throwable e) {
        responseFuture.setFailure(e);
      }
    });
  }

  protected CompletableFuture<PortableHttpResponse> sendRequest(
      Instance storageNode,
      VenicePath path,
      Executor executor) throws RouterException {

    String storeName = path.getStoreName();
    RequestType requestType = path.getRequestType();

    if (!pendingRequestThrottler.put()) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.of(storeName),
          Optional.of(requestType),
          SERVICE_UNAVAILABLE,
          "Maximum number of pending request threshold reached! Please contact Venice team.");
    }
    routerStats.recordPendingRequest(storageNode.getNodeId());

    long startTime = System.nanoTime();
    CompletableFuture<PortableHttpResponse> responseFuture = new CompletableFuture<>();

    try {
      storageNodeClient.query(
          storageNode,
          path,
          responseFuture::complete,
          responseFuture::completeExceptionally,
          () -> responseFuture.cancel(false),
          startTime);

    } catch (Throwable throwable) {
      responseFuture.completeExceptionally(throwable);

    } finally {
      return responseFuture.whenCompleteAsync((response, throwable) -> {
        RouteHttpStats perRouteStats = perRouteStatsByType.getStatsByType(requestType);
        perRouteStats.recordResponseWaitingTime(storageNode.getHost(), LatencyUtils.getLatencyInMS(startTime));

        routerStats.recordFinishedRequest(storageNode.getNodeId());
        pendingRequestThrottler.take();
      }, executor);
    }
  }

  protected FullHttpResponse buildResponse(VenicePath path, PortableHttpResponse serverResponse, byte[] serverResponseContent) {
    int statusCode = serverResponse.getStatusCode();
    if (PASS_THROUGH_ERROR_CODES.contains(statusCode)) {
      return buildPlainTextResponse(HttpResponseStatus.valueOf(statusCode), serverResponseContent);
    }

    ByteBuf content = Unpooled.wrappedBuffer(serverResponseContent);
    CompressionStrategy contentCompression =
        VeniceResponseDecompressor.getCompressionStrategy(serverResponse.getFirstHeader(VENICE_COMPRESSION_STRATEGY));

    if (statusCode == HttpStatus.SC_OK && path.isStreamingRequest()) {
      VeniceChunkedResponse chunkedResponse = path.getChunkedResponse().get();
      if (path.getRequestType().equals(RequestType.MULTI_GET_STREAMING)) {
        Pair<ByteBuf, CompressionStrategy> chunk =
            path.getResponseDecompressor().processMultiGetResponseForStreaming(contentCompression, content);
        chunkedResponse.write(chunk.getFirst(), chunk.getSecond());
      } else {
        chunkedResponse.write(content);
      }
      content = Unpooled.EMPTY_BUFFER;
    }

    if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_NOT_FOUND) {
      statusCode = HttpStatus.SC_BAD_GATEWAY;
    }

    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(statusCode), content);
    response.headers()
        .set(HttpHeaderNames.CONTENT_TYPE, serverResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE))
        .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())
        .set(HttpConstants.VENICE_SCHEMA_ID, serverResponse.getFirstHeader(HttpConstants.VENICE_SCHEMA_ID))
        .set(HttpConstants.VENICE_COMPRESSION_STRATEGY, contentCompression.getValue());
    return response;
  }

  protected FullHttpResponse buildPlainTextResponse(HttpResponseStatus status, byte[] content) {
    return buildPlainTextResponse(status, Unpooled.wrappedBuffer(content));
  }

  protected FullHttpResponse buildPlainTextResponse(HttpResponseStatus status, ByteBuf content) {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
    response.headers()
        .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.TEXT_PLAIN)
        .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    return response;
  }

  protected boolean isCacheEnabled(VenicePath path) {
    if (!routerCache.isPresent() || !path.getRequestType().equals(RequestType.SINGLE_GET)) {
      return false;
    }
    return storeRepository.isSingleGetRouterCacheEnabled(path.getStoreName());
  }

  /**
   * Handle cache lookup for both batch-get and single-get request, and this function is handling throttling as well.
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
   * @return whether the single-get hits cache or all keys in batch-get hit cache
   * @throws RouterException
   */
  protected boolean handleCacheLookupAndThrottling(
      VenicePath path,
      Instance selectedHost,
      AsyncPromise<List<FullHttpResponse>> responseFuture) throws RouterException {

    RequestType requestType = path.getRequestType();
    if (requestType.equals(RequestType.COMPUTE) || path.isStreamingRequest()) {
      /**
       * Router cache is not supported for read compute yet;
       * the feature vector to compute against could change, so even though the key is the same, the compute result
       * could be different.
       *
       * Disable cache for streaming request since multi-get cache is never enabled in prod, and we would like to
       * avoid unnecessary caching logic, which is complicate.
       */
      return false;
    }

    String storeName = path.getStoreName();
    AggRouterHttpRequestStats stats = perStoreStatsByType.getStatsByType(requestType);
    List<MultiGetResponseRecordV1> cacheResultForMultiGet = new ArrayList<>();

    try {
      if (!isCacheEnabled(path)) {
        if (!path.isRetryRequest() && requestType.equals(RequestType.SINGLE_GET)) {
          readRequestThrottler.mayThrottleRead(storeName, readRequestThrottler.getReadCapacity(), Optional.of(selectedHost.getNodeId()));
        }
        return false;
      }

      long startTimeInNS = System.nanoTime();
      switch (requestType) {
        case SINGLE_GET:
          /**
           * Cache throttling first
           * Only throttle in store level since the cache lookup request is not actually sending to any storage node
           */
          if (!path.isRetryRequest()) {
            readRequestThrottler.mayThrottleRead(storeName, cacheHitRequestThrottleWeight * readRequestThrottler.getReadCapacity(), Optional.empty());
          }

          stats.recordCacheLookupRequest(storeName);
          Optional<RouterCache.CacheValue> cacheValue =
              routerCache.get().get(storeName, path.getVersionNumber(), path.getPartitionKey().getKeyBuffer());

          stats.recordCacheLookupLatency(storeName, LatencyUtils.getLatencyInMS(startTimeInNS));
          if (cacheValue != null) {
            // Cache hit
            stats.recordCacheHitRequest(storeName);
            FullHttpResponse response;
            if (cacheValue.isPresent()) {
              response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                  Unpooled.wrappedBuffer(cacheValue.get().getValue()));
              response.headers()
                  .set(HttpConstants.VENICE_PARTITION, ((VeniceSingleGetPath)path).getPartition())
                  .set(HttpConstants.VENICE_SCHEMA_ID, cacheValue.get().getSchemaId())
                  .set(HttpHeaderNames.CONTENT_LENGTH, cacheValue.get().getValue().length)
                  .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);
            } else {
              response =
                  new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, Unpooled.EMPTY_BUFFER);
              response.headers()
                  .set(HttpHeaderNames.CONTENT_LENGTH, 0)
                  .set(HttpConstants.VENICE_PARTITION, ((VeniceSingleGetPath)path).getPartition());
            }

            response.headers().set(VENICE_COMPRESSION_STRATEGY, routerCache.get().getCompressionStrategy(path.getResourceName()).getValue());

            responseFuture.setSuccess(Collections.singletonList(response));
            return true;
          }

          /**
           * Cache miss or the value is expired
           * Unset the previous per-store throttler
           */
          if (!path.isRetryRequest()) {
            readRequestThrottler.mayThrottleRead(storeName, -cacheHitRequestThrottleWeight * readRequestThrottler.getReadCapacity(), Optional.empty());
            readRequestThrottler.mayThrottleRead(storeName, readRequestThrottler.getReadCapacity(), Optional.of(selectedHost.getNodeId()));
          }
          return false;

        case MULTI_GET:
          /**
           * Don't throttle because the throttling for multi-key request is already done in {@link VeniceDelegateMode}.
           */

          int valueSchemaId = 0;
          int cacheHitTimes = 0;
          for (Map.Entry<Integer, RouterKey> routerKeyEntry : ((VeniceMultiGetPath)path).getKeyIdxToRouterKeySet()) {
            stats.recordCacheLookupRequest(storeName);
            long cacheLookupStartTimeInNS = System.nanoTime();
            Optional<RouterCache.CacheValue> cacheValueForOneKey =
                routerCache.get().get(storeName, path.getVersionNumber(), routerKeyEntry.getValue().getKeyBuffer());
            stats.recordCacheLookupLatencyForEachKeyInMultiget(storeName, LatencyUtils.getLatencyInMS(cacheLookupStartTimeInNS));
            if (cacheValueForOneKey != null) {
              // cache hit for this key
              stats.recordCacheHitRequest(storeName);
              cacheHitTimes++;
              if (cacheValueForOneKey.isPresent()) {
                MultiGetResponseRecordV1 multiGetResponseRecordV1 = new MultiGetResponseRecordV1();
                multiGetResponseRecordV1.value = cacheValueForOneKey.get().getByteBuffer();
                multiGetResponseRecordV1.keyIndex = routerKeyEntry.getKey();
                multiGetResponseRecordV1.schemaId = cacheValueForOneKey.get().getSchemaId();
                valueSchemaId = Math.max(valueSchemaId, multiGetResponseRecordV1.schemaId);

                // add cache value to result
                cacheResultForMultiGet.add(multiGetResponseRecordV1);
              }

              // remove this key request in MultiGetPath
              ((VeniceMultiGetPath)path).removeFromRequest(routerKeyEntry.getValue());
            }
          }

          // Do throttling discount for all the keys that are found in the cache
          if (!path.isRetryRequest()) {
            readRequestThrottler.mayThrottleRead(storeName,
                -cacheHitRequestThrottleWeight * readRequestThrottler.getReadCapacity() * cacheHitTimes, Optional.empty());
          }
          stats.recordCacheLookupLatency(storeName, LatencyUtils.getLatencyInMS(startTimeInNS));

          if (((VeniceMultiGetPath)path).isEmptyRequest()) {
            // all keys hit cache
            byte[] contentToByte = multiGetResponseRecordSerializer.serializeObjects(cacheResultForMultiGet);
            ByteBuf content = Unpooled.wrappedBuffer(contentToByte);
            CompressionStrategy compressionStrategy = routerCache.get().getCompressionStrategy(path.getResourceName());

            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);

            response.headers()
                .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
                .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY)
                .set(HttpConstants.VENICE_SCHEMA_ID, valueSchemaId)
                .set(HttpConstants.VENICE_COMPRESSION_STRATEGY, compressionStrategy.getValue());

            responseFuture.setSuccess(Collections.singletonList(response));
            return true;
          }

          // part of the key hit cache; still need to send a batch-get request
          return false;
      }

    } catch (QuotaExceededException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.of(storeName),
          Optional.of(requestType),
          TOO_MANY_REQUESTS,
          "Read quota exceeded: " + e.getMessage());
    }
    return false;
  }

  protected void updateCache(VenicePath path, PortableHttpResponse serverResponse, byte[] serverResponseContent) {
    if (!isCacheEnabled(path)) {
      return;
    }

    int statusCode = serverResponse.getStatusCode();
    if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_NOT_FOUND) {
      return;
    }

    routerCache.get().setCompressionType(
        path.getResourceName(),
        VeniceResponseDecompressor.getCompressionStrategy(serverResponse.getFirstHeader(VENICE_COMPRESSION_STRATEGY)));

    Optional<RouterCache.CacheValue> value;
    if (statusCode == HttpStatus.SC_OK) {
      int valueSchemaId = Integer.parseInt(serverResponse.getFirstHeader(HttpConstants.VENICE_SCHEMA_ID));
      value = Optional.of(new RouterCache.CacheValue(ByteBuffer.wrap(serverResponseContent), valueSchemaId));
    } else {
      value = Optional.empty();
    }

    long startTime = System.nanoTime();
    String storeName = path.getStoreName();
    routerCache.get().put(storeName, path.getVersionNumber(), path.getPartitionKey().getKeyBuffer(), value);

    AggRouterHttpRequestStats perStoreStats = perStoreStatsByType.getStatsByType(path.getRequestType());
    perStoreStats.recordCachePutRequest(storeName);
    perStoreStats.recordCachePutLatency(storeName, LatencyUtils.getLatencyInMS(startTime));
  }
}
