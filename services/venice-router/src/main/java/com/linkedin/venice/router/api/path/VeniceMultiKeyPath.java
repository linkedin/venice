package com.linkedin.venice.router.api.path;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.meta.StoreVersionName;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.RouterRetryConfig;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VeniceResponseDecompressor;
import com.linkedin.venice.router.exception.VeniceKeyCountLimitException;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.VeniceChunkedResponse;
import com.linkedin.venice.router.streaming.VeniceChunkedWriteHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;


/**
 * Multi-key requests support many additional functionalities compared to single gets, including:
 *
 * - Smart long-tail retry configurations
 * - Streaming responses
 * - Helix-assisted routing (HAR)
 *
 * @param <K> the key wrapper to be sent to servers for the given type of query
 */
public abstract class VeniceMultiKeyPath<K> extends VenicePath {
  private static final int NOT_INITIALIZED = -1;
  private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong(0);

  protected final Map<RouterKey, K> routerKeyMap;
  private int longTailRetryThresholdMs = NOT_INITIALIZED;
  /** Will be non-null if the request supports streaming */
  private VeniceChunkedResponse chunkedResponse = null;
  private AtomicInteger currentAllowedRetryRouteCnt = new AtomicInteger(0);
  private volatile long requestId = NOT_INITIALIZED;
  private int helixGroupId = NOT_INITIALIZED;

  public VeniceMultiKeyPath(
      StoreVersionName storeVersionName,
      RouterRetryConfig retryConfig,
      RetryManager retryManager,
      VeniceResponseDecompressor responseDecompressor) {
    // HashMap's performance is better than TreeMap
    this(storeVersionName, new HashMap<>(), retryConfig, retryManager, responseDecompressor);
  }

  public VeniceMultiKeyPath(
      StoreVersionName storeVersionName,
      Map<RouterKey, K> routerKeyMap,
      RouterRetryConfig retryConfig,
      RetryManager retryManager,
      VeniceResponseDecompressor responseDecompressor) {
    super(storeVersionName, retryConfig, retryManager, responseDecompressor);
    this.routerKeyMap = routerKeyMap;
  }

  /**
   * Fill the router key map and the index2routerKey map.
   *
   * @param resourceName
   * @param keys            Multiple keys from client request; keys have been deserialized to ByteBuffer
   * @param partitionFinder
   * @param maxKeyCount
   * @throws RouterException
   */
  public void initialize(
      String resourceName,
      List<ByteBuffer> keys,
      VenicePartitionFinder partitionFinder,
      int maxKeyCount,
      AggRouterHttpRequestStats stats) throws RouterException {
    int keyIdx = 0;
    int partitionNum;
    VenicePartitioner partitioner;
    try {
      partitionNum = partitionFinder.getNumPartitions(resourceName);
      partitioner = partitionFinder.findPartitioner(getStoreName(), getVersionNumber());
    } catch (VeniceNoHelixResourceException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTrackingResourceNotFound(
          getStoreName(),
          RequestType.COMPUTE,
          e.getHttpResponseStatus(),
          e.getMessage());
    }

    int keyCount = keys.size();
    if (keyCount > maxKeyCount) {
      throw new VeniceKeyCountLimitException(getStoreName(), getRequestType(), keyCount, maxKeyCount);
    }
    if (keyCount == 0) {
      /**
       * TODO: Right now, there is no good way to return empty response if the key set is empty.
       * The logic to handle empty key here is different from client side, since the client will return empty map
       * instead of throwing an exception.
       *
       * If application is using Venice client to send batch-get request, this piece of logic shouldn't be triggered.
       */
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          getStoreName(),
          getRequestType(),
          BAD_REQUEST,
          "Key count in multi-get request should not be zero");
    }

    for (ByteBuffer key: keys) {
      RouterKey routerKey = new RouterKey(key);

      stats.recordKeySize(key.remaining());

      // partition lookup
      int partitionId = partitioner.getPartitionId(routerKey.getKeyBuffer(), partitionNum);
      routerKey.setPartitionId(partitionId);
      K routerRequestKey = createRouterRequestKey(key, keyIdx, partitionId);
      this.routerKeyMap.put(routerKey, routerRequestKey);
      ++keyIdx;
    }
    setPartitionKeys(this.routerKeyMap.keySet());
  }

  /**
   * The following function is used to generate a {@link VeniceMultiGetPath} or a {@link VeniceComputePath} for a given key,
   * and the generated {@link VenicePath} will be used to compose router request and forward to storage node;
   *
   * This function will be triggered by DDS router framework.
   *
   * @param s
   * @return
   */
  @Override
  public VenicePath substitutePartitionKey(RouterKey s) {
    K routerRequestKey = routerKeyMap.get(s);
    if (routerRequestKey == null) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
          getStoreName(),
          getRequestType(),
          BAD_GATEWAY,
          "RouterKey: " + s + " should exist in the original path");
    }
    Map<RouterKey, K> newRouterKeyMap = new HashMap<>();

    newRouterKeyMap.put(s, routerRequestKey);

    return fixRetryRequestForSubPath(newRouterKeyMap);
  }

  /**
   * The following function is used to generate a {@link VeniceMultiGetPath} or a {@link VeniceComputePath} for a given key subset,
   * and the generated {@link VenicePath} will be used to compose router request and forward to storage node;
   *
   * This function will be triggered by DDS router framework.
   *
   * @param s
   * @return
   */
  @Override
  public VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s) {
    Map<RouterKey, K> newRouterKeyMap = new HashMap<>(s.size());
    for (RouterKey key: s) {
      /**
       * Using {@link Map#get(Object)} and checking whether it is null is faster than the following statements:
       * if (!routerKeyMap.containsKey(key)) {
       *   ... ...
       * }
       * newRouterKeyMap.put(key, routerKeyMap.get(key));
       * newKeyIdxToRouterKey.put(routerKeyMap.get(key).keyIndex, key);
       *
       * Since this way will save two unnecessary Map lookups.
       * This could make a big difference considering large batch-get user cases.
       */
      K routerRequestKey = routerKeyMap.get(key);
      if (routerRequestKey == null) {
        throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
            getStoreName(),
            getRequestType(),
            BAD_GATEWAY,
            "RouterKey: " + key + " should exist in the original path");
      }

      newRouterKeyMap.put(key, routerRequestKey);
    }

    return fixRetryRequestForSubPath(newRouterKeyMap);
  }

  @Override
  public HttpUriRequest composeRouterRequestInternal(String storageNodeUri) {
    HttpPost routerRequest = new HttpPost(storageNodeUri + getLocation());
    /**
     * Use {@link ByteArrayEntity} here instead of {@link BasicHttpEntity} to explicitly disable
     * streaming (chunked transfer-encoding) since the streaming might cause some inefficiency
     * in the storage node.
     */
    ByteArrayEntity entity = new ByteArrayEntity(serializeRouterRequest());
    routerRequest.setEntity(entity);

    return routerRequest;
  }

  @Override
  public HttpMethod getHttpMethod() {
    return HttpMethod.POST;
  }

  @Override
  public byte[] getBody() {
    return serializeRouterRequest();
  }

  public int getLongTailRetryMaxRouteForMultiKeyReq() {
    return this.retryConfig.getLongTailRetryMaxRouteForMultiKeyReq();
  }

  @Override
  public boolean isLongTailRetryAllowedForNewRequest() {
    int longTailRetryMaxRouteForMultiKeyReq = getLongTailRetryMaxRouteForMultiKeyReq();
    if (longTailRetryMaxRouteForMultiKeyReq == -1) {
      // feature is disabled
      return true;
    }
    if (longTailRetryMaxRouteForMultiKeyReq <= 0) {
      return false;
    }
    return currentAllowedRetryRouteCnt.incrementAndGet() <= longTailRetryMaxRouteForMultiKeyReq;
  }

  @Override
  protected void setupRetryRelatedInfo(VenicePath originalPath) {
    super.setupRetryRelatedInfo(originalPath);
    if (!(originalPath instanceof VeniceMultiKeyPath)) {
      throw new VeniceException("Expected `VeniceMultiKeyPath` type here, but found: " + originalPath.getClass());
    }
    this.longTailRetryThresholdMs = originalPath.getLongTailRetryThresholdMs();
    this.chunkedResponse = originalPath.getChunkedResponse();
    /**
     * We need to share the {@link #currentAllowedRetryRouteCnt} across all the sub paths.
     */
    this.currentAllowedRetryRouteCnt = ((VeniceMultiKeyPath) originalPath).currentAllowedRetryRouteCnt;
    this.requestId = originalPath.getRequestId();
    this.helixGroupId = originalPath.getHelixGroupId();
  }

  @Override
  public boolean isSmartLongTailRetryEnabled() {
    return this.retryConfig.isSmartLongTailRetryEnabled();
  }

  @Override
  public int getSmartLongTailRetryAbortThresholdMs() {
    return this.retryConfig.getSmartLongTailRetryAbortThresholdMs();
  }

  @Override
  public int getLongTailRetryThresholdMs() {
    if (this.longTailRetryThresholdMs != NOT_INITIALIZED) {
      return this.longTailRetryThresholdMs;
    }
    Collection<RouterKey> keys = getPartitionKeys();
    if (keys == null) {
      throw new IllegalStateException("partitionKeys not initialized!");
    }
    /**
     * Long tail retry threshold is based on key count for batch-get request.
     */
    if (keys.isEmpty()) {
      // Should not happen
      throw new IllegalStateException("Met scatter-gather request without any keys");
    }
    /**
     * Refer to {@link ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS} to get more info.
     */
    TreeMap<Integer, Integer> longTailRetryConfigForBatchGet = retryConfig.getLongTailRetryForBatchGetThresholdMs();
    Map.Entry<Integer, Integer> floorEntry = longTailRetryConfigForBatchGet.floorEntry(keys.size());
    if (floorEntry == null) {
      throw new IllegalStateException("getLongTailRetryForBatchGetThresholdMs() returned a map with no floor entry!");
    }
    return floorEntry.getValue();
  }

  @Override
  public long getRequestId() {
    if (this.requestId == NOT_INITIALIZED) {
      synchronized (this) {
        if (this.requestId == NOT_INITIALIZED) {
          this.requestId = REQUEST_ID_GENERATOR.getAndIncrement();
        }
      }
    }
    return this.requestId;
  }

  @Override
  public int getHelixGroupId() {
    return this.helixGroupId;
  }

  @Override
  public void setHelixGroupId(int helixGroupId) {
    this.helixGroupId = helixGroupId;
  }

  @Override
  public void setChunkedWriteHandler(
      ChannelHandlerContext ctx,
      VeniceChunkedWriteHandler chunkedWriteHandler,
      RouterStats<AggRouterHttpRequestStats> routerStats) {
    if (this.chunkedResponse != null) {
      // Defensive code
      throw new IllegalStateException("VeniceChunkedWriteHandler has already been setup");
    }
    this.chunkedResponse = new VeniceChunkedResponse(
        this.storeVersionName.getStoreName(),
        getStreamingRequestType(),
        ctx,
        chunkedWriteHandler,
        routerStats,
        getClientComputeHeader());
  }

  @Override
  public VeniceChunkedResponse getChunkedResponse() {
    return this.chunkedResponse;
  }

  /**
   * Create a router request key.
   *
   * @param key
   * @param keyIdx
   * @param partitionId
   * @return An instance of ComputeRouterRequestKeyV1 for compute request; return an instance of MultiGetRouterRequestKeyV1 for multi-get
   */
  protected abstract K createRouterRequestKey(ByteBuffer key, int keyIdx, int partitionId);

  /**
   * @param routerKeyMap
   * @return a sub-path with a new set of keys
   */
  protected abstract VenicePath fixRetryRequestForSubPath(Map<RouterKey, K> routerKeyMap);

  /**
   * For multi-get requests, simply serialize the set of RouterKey to bytes;
   * for read compute requests, concatenate the compute request and the serialized keys
   * @return
   */
  protected abstract byte[] serializeRouterRequest();
}
