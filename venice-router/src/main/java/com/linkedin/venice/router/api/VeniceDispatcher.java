package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.AsyncPromise;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.PartitionDispatchHandler4;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.common.PartitionOffsetMapUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.VeniceChunkedResponse;
import com.linkedin.venice.router.api.path.VeniceMultiGetPath;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.api.path.VeniceSingleGetPath;
import com.linkedin.venice.router.cache.RouterCache;
import com.linkedin.venice.router.httpclient.PortableHttpResponse;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpStats;
import com.linkedin.venice.router.throttle.PendingRequestThrottler;
import com.linkedin.venice.router.throttle.RouterThrottler;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import static com.linkedin.venice.HttpConstants.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


public class VeniceDispatcher implements PartitionDispatchHandler4<Instance, VenicePath, RouterKey>, Closeable{
  private static final Logger logger = Logger.getLogger(VeniceDispatcher.class);

  // key is (resource + "_" + partition)
  private final ConcurrentMap<String, Long> offsets = new ConcurrentHashMap<>();
  private final VeniceHostHealth healthMonitor;

   // How many offsets behind can a storage node be for a partition and still be considered 'caught up'
  private long acceptableOffsetLag = 10000; /* TODO: make this configurable for streaming use-case */

  private final ReadOnlyStoreRepository storeRepository;
  private final Optional<RouterCache> routerCache;
  private final double cacheHitRequestThrottleWeight;
  private static final ByteBuf NOT_FOUND_CONTENT = Unpooled.wrappedBuffer(new byte[0]);

  private final RouterStats<AggRouterHttpRequestStats> routerStats;
  private final RouterStats<RouteHttpStats> perRouteStats;

  private static List<Integer> passThroughErrorCodes = Arrays.asList(new Integer[]{
      HttpResponseStatus.TOO_MANY_REQUESTS.code()
  });

  /**
   * Single-get throttling needs to happen here because of caching.
   */
  private RouterThrottler readRequestThrottler;

  private final RecordDeserializer<MultiGetResponseRecordV1> multiGetResponseRecordDeserializer =
      SerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class);
  private final RecordSerializer<MultiGetResponseRecordV1> multiGetResponseRecordSerializer =
      SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);

  private final StorageNodeClient storageNodeClient;

  private final PendingRequestThrottler pendingRequestThrottler;

  public VeniceDispatcher(VeniceRouterConfig config, VeniceHostHealth healthMonitor,
      ReadOnlyStoreRepository storeRepository, Optional<RouterCache> routerCache,
      RouterStats routerStats, MetricsRepository metricsRepository,
      StorageNodeClient storageNodeClient) {
    this.healthMonitor = healthMonitor;
    this.storeRepository = storeRepository;
    this.routerCache = routerCache;
    this.cacheHitRequestThrottleWeight = config.getCacheHitRequestThrottleWeight();
    this.routerStats = routerStats;
    this.perRouteStats = new RouterStats<>( requestType -> new RouteHttpStats(metricsRepository, requestType) );

    this.storageNodeClient = storageNodeClient;

    this.pendingRequestThrottler = new PendingRequestThrottler(config.getMaxPendingRequest());
  }

  public void initReadRequestThrottler(RouterThrottler requestThrottler) {
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

    AggRouterHttpRequestStats aggStats = routerStats.getStatsByType(path.getRequestType());

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

    List<MultiGetResponseRecordV1> cacheResultForMultiGet = new ArrayList<>();
    if (handleCacheLookupAndThrottling(path, host, responseFuture, contextExecutor, cacheResultForMultiGet, aggStats)) {
      // Single get hits cache or all keys in batch get hit cache
      return;
    }

    // TODO: add metric for this pending request
    if (!pendingRequestThrottler.put()) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(path.getRequestType()),
          SERVICE_UNAVAILABLE, "Maximum number of pending request threshold reached! Please contact Venice team.");
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Routing request to host: " + host.getHost() + ":" + host.getPort());
    }

    RequestType requestType = path.getRequestType();
    if (requestType.equals(RequestType.COMPUTE) || requestType.equals(RequestType.COMPUTE_STREAMING)) {
      // check whether the store is enabled for compute
      if (!storeRepository.isReadComputationEnabled(storeName)) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName),
            Optional.of(requestType), METHOD_NOT_ALLOWED,
            "Your store is not enabled for read computations, please contact Venice team if you would like to enable compute feature");
      }
    }
    RouteHttpStats currentRouteStat = perRouteStats.getStatsByType(path.getRequestType());

    String address = host.getHost() + ":" + host.getHost() + "/";

    Consumer<PortableHttpResponse> completedCallBack = new Consumer<PortableHttpResponse>() {
      @Override
      public void accept(PortableHttpResponse result) {
        pendingRequestThrottler.take();
        path.markStorageNodeAsFast(host.getNodeId());
        int statusCode = result.getStatusCode();
        if (statusCode == HttpStatus.SC_INTERNAL_SERVER_ERROR || statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
          // Retry errored request
          if (!path.isRetryRequest()) {
            // Together with long-tail retry, for a single scatter request, it is possible to have at most two
            // retry requests, one triggered by long-tail retry threshold, the other one is triggered by error
            // response.
            contextExecutor.execute(() -> {
              // Triggers an immediate router retry excluding the host we selected.
              retryFuture.setSuccess(HttpResponseStatus.valueOf(statusCode));
            });
            return;
          }
        }

        Set<String> partitionNames = part.getPartitionsNames();
        String resourceName = path.getResourceName();
        RequestType requestType = path.getRequestType();

        // TODO: make this logic consistent across single-get and multi-get
        switch (requestType) {
          case SINGLE_GET:
            Iterator<String> partitionIterator = partitionNames.iterator();
            String partitionName = partitionIterator.next();
            if (partitionIterator.hasNext()) {
              logger.error(
                  "There must be only one partition in a request, handling request as if there is only one partition. "
                      + "Partitions in request: " + String.join(",", partitionNames));
            }
            if (statusCode == HttpStatus.SC_OK) {
              if (null == result.getFirstHeader(HttpConstants.VENICE_OFFSET)) {
                throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(RequestType.SINGLE_GET),
                    INTERNAL_SERVER_ERROR, "Header: " + HttpConstants.VENICE_OFFSET + " in the response from storage node is expected for address: " + address);
              }
              String offsetHeader = result.getFirstHeader(HttpConstants.VENICE_OFFSET);
              long offset = Long.parseLong(offsetHeader);
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
          case MULTI_GET_STREAMING:
          case COMPUTE_STREAMING:
          case COMPUTE:
            // Get partition offset header
            if (statusCode == HttpStatus.SC_OK) {
              if (null == result.getFirstHeader(HttpConstants.VENICE_OFFSET)) {
                throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(requestType),
                    INTERNAL_SERVER_ERROR, "Header: " + HttpConstants.VENICE_OFFSET + " in the response from storage node is expected for address: " + address);
              }
              String offsetHeader = result.getFirstHeader(HttpConstants.VENICE_OFFSET);
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
                    logger.error(requestType.name() + " response doesn't contain offset for partition: " + pName);
                  }
                });
              } catch (IOException e) {
                logger.error("Failed to parse partition offset map from content: " + offsetHeader);
              }
            }
            break;
        }

        int responseStatus = result.getStatusCode();
        FullHttpResponse response;
        byte[] contentToByte;

        try {
          contentToByte = result.getContentInBytes();
        } catch (Exception e) {
          completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
          return;
        }

        if (passThroughErrorCodes.contains(responseStatus)) {
          completeWithError(HttpResponseStatus.valueOf(responseStatus), contentToByte);
        }
        int valueSchemaId = Integer.parseInt(result.getFirstHeader(HttpConstants.VENICE_SCHEMA_ID));
        CompressionStrategy compressionStrategy =
            result.containsHeader(VENICE_COMPRESSION_STRATEGY) ? CompressionStrategy.valueOf(Integer.valueOf(result.getFirstHeader(VENICE_COMPRESSION_STRATEGY))) : CompressionStrategy.NO_OP;

        ByteBuf content;
        if (requestType.equals(RequestType.MULTI_GET)) {
          if (cacheResultForMultiGet.size() > 0) {
            if (contentToByte.length > 0) {
              // combine the cache results with the request results
              CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer(2);
              compositeByteBuf.addComponent(Unpooled.wrappedBuffer(contentToByte));
              long serializationStartTimeInNS = System.nanoTime();
              compositeByteBuf.addComponent(Unpooled.wrappedBuffer(multiGetResponseRecordSerializer.serializeObjects(cacheResultForMultiGet)));
              content = compositeByteBuf;
              aggStats.recordCacheResultSerializationLatency(storeName, LatencyUtils.getLatencyInMS(serializationStartTimeInNS));
            } else {
              // server responds with nothing
              long serializationStartTimeInNS = System.nanoTime();
              content = Unpooled.wrappedBuffer(multiGetResponseRecordSerializer.serializeObjects(cacheResultForMultiGet));
              aggStats.recordCacheResultSerializationLatency(storeName, LatencyUtils.getLatencyInMS(serializationStartTimeInNS));
            }
          } else {
            // only put the response from server node to content
            content = Unpooled.wrappedBuffer(contentToByte);
          }
        } else {
          // for single get, the result is only from the server node; otherwise, it would have hit cache and return
          content = Unpooled.wrappedBuffer(contentToByte);
        }

        switch (responseStatus) {
          case HttpStatus.SC_OK:
            /**
             * For streaming support, inside {@link VeniceDispatcher} for each scatter request,
             * this will only send out 'OK' response, and the errored response received from
             * Storage Node will be handled together in {@link VeniceResponseAggregator}.
             */
            Optional<VeniceChunkedResponse> chunkedResponse = path.getChunkedResponse();
            if (chunkedResponse.isPresent()) {
              if (requestType.equals(RequestType.MULTI_GET_STREAMING)) {
                CompressionStrategy responseCompression = VeniceResponseDecompressor.getCompressionStrategy(result.getFirstHeader(VENICE_COMPRESSION_STRATEGY));
                Pair<ByteBuf, CompressionStrategy> responseInfo =
                    path.getResponseDecompressor().processMultiGetResponseForStreaming(responseCompression, content);
                chunkedResponse.get().write(responseInfo.getFirst(), responseInfo.getSecond());
              } else {
                chunkedResponse.get().write(content);
              }
              /**
               * Since here has already sent out the actual payload, only the response meta is needed in
               * {@link VeniceResponseAggregator}.
               */
              response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, VeniceChunkedResponse.EMPTY_BYTE_BUF);
            } else {
              response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
            }
            break;
          case HttpStatus.SC_NOT_FOUND:
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, content);
            break;
          case HttpStatus.SC_INTERNAL_SERVER_ERROR:
          default: //Path Parser will throw BAD_REQUEST responses.
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, BAD_GATEWAY, content);
        }

        response.headers()
            .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())
            .set(HttpHeaderNames.CONTENT_TYPE, result.getFirstHeader(HttpHeaders.CONTENT_TYPE))
            .set(HttpConstants.VENICE_SCHEMA_ID, valueSchemaId)
        /**
         * Keep the original compression strategy header, so that {@link VeniceResponseAggregator} could validate
         * whether the compression strategy is consistent across all the responses returned by storage node.
          */
            .set(HttpConstants.VENICE_COMPRESSION_STRATEGY, compressionStrategy.getValue());
        if (requestType.equals(RequestType.SINGLE_GET)) {
          // For multi-get, the partition is not returned to client
          String partitionIdStr = numberFromPartitionName(partitionNames.iterator().next());
          response.headers().set(HttpConstants.VENICE_PARTITION, partitionIdStr);
        }

        /**
         * The following codes will tie up the client thread after the response has been sent.
         *
         * we have tried performing the cache update asynchronously outside the client's thread by
         * submitting the following codes to an executor, but the executor caused a lot of extra GC
         * so it's not worth adding another executor here.
         *
         * It might still be possible to tune the asynchronous method further in the future, if need.
         */
        if (routerCache.isPresent() && !requestType.equals(RequestType.COMPUTE)) {
          boolean cacheEnabledForStore = requestType.equals(RequestType.SINGLE_GET) ?
              storeRepository.isSingleGetRouterCacheEnabled(storeName) : storeRepository.isBatchGetRouterCacheEnabled(storeName);

          if (cacheEnabledForStore) {
            if (requestType.equals(RequestType.MULTI_GET)) {
              long responseDeserializationStartTimeInNS = System.nanoTime();
              Iterable<MultiGetResponseRecordV1> records = multiGetResponseRecordDeserializer.deserializeObjects(contentToByte);
              aggStats.recordResponseResultsDeserializationLatency(storeName, LatencyUtils.getLatencyInMS(responseDeserializationStartTimeInNS));
              long cacheUpdateStartTimeInNS = System.nanoTime();
              for (MultiGetResponseRecordV1 record : records) {
                // update the cache
                updateCache(path, ((VeniceMultiGetPath) path).getRouterKeyByKeyIdx(record.keyIndex),
                    Optional.of(record.value.array()), Optional.of(valueSchemaId), compressionStrategy, aggStats);
              }
              aggStats.recordCacheUpdateLatencyForMultiGet(storeName, LatencyUtils.getLatencyInMS(cacheUpdateStartTimeInNS));
            } else if (requestType.equals(RequestType.SINGLE_GET)){
              // Update cache for single-get request
              if (responseStatus == HttpStatus.SC_OK) {
                updateCache(path, path.getPartitionKey(), Optional.of(contentToByte), Optional.of(valueSchemaId),
                    compressionStrategy, aggStats);
              } else if (responseStatus == HttpStatus.SC_NOT_FOUND) {
                updateCache(path, path.getPartitionKey(), Optional.empty(), Optional.empty(), compressionStrategy, aggStats);
              }
            }
          }
        }
        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });
        recordResponseWaitingTime(host.getHost(), currentRouteStat, dispatchStartTSInNS);
      }

      private void completeWithError(HttpResponseStatus status, Throwable e) {
        String errMsg = e.getMessage();
        if (null == errMsg) {
          errMsg = "Unknown error, caught: " + e.getClass().getCanonicalName();
        }
        ByteBuf content = Unpooled.wrappedBuffer(errMsg.getBytes(StandardCharsets.UTF_8));
        completeWithError(status, content);
      }

      private void completeWithError(HttpResponseStatus status, ByteBuf content) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.TEXT_PLAIN)
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });
      }

      private void completeWithError(HttpResponseStatus status, byte[] contentByteArray) {
        ByteBuf content = Unpooled.wrappedBuffer(contentByteArray);
        completeWithError(status, content);
      }
    };

    Consumer<Throwable> failedCallBack = new Consumer<Throwable>() {
      @Override
      public void accept(Throwable ex) {
        pendingRequestThrottler.take();
        completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
        recordResponseWaitingTime(host.getHost(), currentRouteStat, dispatchStartTSInNS);
      }

      private void completeWithError(HttpResponseStatus status, Throwable e) {
        String errMsg = e.getMessage();
        if (null == errMsg) {
          errMsg = "Unknown error, caught: " + e.getClass().getCanonicalName();
        }
        ByteBuf content = Unpooled.wrappedBuffer(errMsg.getBytes(StandardCharsets.UTF_8));
        completeWithError(status, content);
      }

      private void completeWithError(HttpResponseStatus status, ByteBuf content) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.TEXT_PLAIN)
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });
      }
    };

    BooleanSupplier cancelledCallBack = new BooleanSupplier() {
      @Override
      public boolean getAsBoolean() {
        pendingRequestThrottler.take();
        completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR, new VeniceException("Request to storage node was cancelled"));
        recordResponseWaitingTime(host.getHost(), currentRouteStat, dispatchStartTSInNS);
        return true;
      }

      private void completeWithError(HttpResponseStatus status, Throwable e) {
        String errMsg = e.getMessage();
        if (null == errMsg) {
          errMsg = "Unknown error, caught: " + e.getClass().getCanonicalName();
        }
        ByteBuf content = Unpooled.wrappedBuffer(errMsg.getBytes(StandardCharsets.UTF_8));
        completeWithError(status, content);
      }

      private void completeWithError(HttpResponseStatus status, ByteBuf content) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.TEXT_PLAIN)
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });
      }
    };

    path.recordOriginalRequestStartTimestamp();
    storageNodeClient.query(host, path, completedCallBack, failedCallBack, cancelledCallBack, dispatchStartTSInNS);
  }

  private void recordResponseWaitingTime(String hostName, RouteHttpStats routeStats, long dispatchStartTSInNS) {
    double latencyInMS = LatencyUtils.getLatencyInMS(dispatchStartTSInNS);
    routeStats.recordResponseWaitingTime(hostName, latencyInMS);
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
   * @param contextExecutor
   * @param cacheResultForMultiGet the cached value for the key will be added to this list
   * @return whether the single-get hits cache or all keys in batch-get hit cache
   * @throws RouterException
   */
  protected boolean handleCacheLookupAndThrottling(VenicePath path, Instance selectedHost,
      AsyncPromise<List<FullHttpResponse>> responseFuture, Executor contextExecutor,
      List<MultiGetResponseRecordV1> cacheResultForMultiGet, AggRouterHttpRequestStats stats) throws RouterException {
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

    try {
      boolean cacheEnabledForStore = routerCache.isPresent();
      cacheEnabledForStore &= requestType.equals(RequestType.SINGLE_GET) ?
          storeRepository.isSingleGetRouterCacheEnabled(storeName) : storeRepository.isBatchGetRouterCacheEnabled(storeName);
      if (!cacheEnabledForStore) {
        // Caching is not enabled
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
                  new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, NOT_FOUND_CONTENT);
              response.headers()
                  .set(HttpHeaderNames.CONTENT_LENGTH, 0)
                  .set(HttpConstants.VENICE_PARTITION, ((VeniceSingleGetPath)path).getPartition());
            }

            response.headers().set(VENICE_COMPRESSION_STRATEGY, routerCache.get().getCompressionStrategy(path.getResourceName()).getValue());

            contextExecutor.execute(() -> {
              responseFuture.setSuccess(Collections.singletonList(response));
            });
            return true;
          } else {
            /**
             * Cache miss or the value is expired
             * Unset the previous per-store throttler
             */
            if (!path.isRetryRequest()) {
              readRequestThrottler.mayThrottleRead(storeName, -cacheHitRequestThrottleWeight * readRequestThrottler.getReadCapacity(), Optional.empty());
              readRequestThrottler.mayThrottleRead(storeName, readRequestThrottler.getReadCapacity(), Optional.of(selectedHost.getNodeId()));
            }
          }
          break;
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

            contextExecutor.execute(() -> {
              responseFuture.setSuccess(Collections.singletonList(response));
            });
            return true;
          } else {
            // part of the key hit cache; still need to send a batch-get request
            return false;
          }
      }

    } catch (QuotaExceededException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(RequestType.SINGLE_GET),
          TOO_MANY_REQUESTS, "Quota exceeds! msg: " + e.getMessage());
    }

    return false;
  }

  /**
   * Update cache
   * @param path
   * @param content If not found, this field will be {@link Optional#empty()}
   * @param valueSchemaId If not found, this field will be {@link Optional#empty()}
   */
  protected void updateCache(VenicePath path,  RouterKey routerKey, Optional<byte[]> content,
      Optional<Integer> valueSchemaId, CompressionStrategy compressionStrategy, AggRouterHttpRequestStats stats) {
    String storeName = path.getStoreName();
    stats.recordCachePutRequest(storeName);

    long startTimeInNS = System.nanoTime();
    try {
      if (content.isPresent() && valueSchemaId.isPresent()) {
        RouterCache.CacheValue cacheValue = new RouterCache.CacheValue(ByteBuffer.wrap(content.get()), valueSchemaId.get());
        routerCache.get().put(storeName, path.getVersionNumber(), routerKey.getKeyBuffer(), Optional.of(cacheValue));
      } else {
        routerCache.get().put(storeName, path.getVersionNumber(), routerKey.getKeyBuffer(), Optional.empty());
      }
      routerCache.get().setCompressionType(path.getResourceName(), compressionStrategy);
    } catch (Exception e) {
      logger.error("Received exception during updating cache", e);
    }
    stats.recordCachePutLatency(storeName, LatencyUtils.getLatencyInMS(startTimeInNS));
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
    storageNodeClient.close();
  }

  protected static String numberFromPartitionName(String partitionName){
    return partitionName.substring(partitionName.lastIndexOf("_")+1);
  }
}
