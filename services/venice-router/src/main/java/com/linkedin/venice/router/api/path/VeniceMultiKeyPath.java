package com.linkedin.venice.router.api.path;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.exception.VeniceKeyCountLimitException;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;


public abstract class VeniceMultiKeyPath<K> extends VenicePath {
  protected int keyNum;
  protected final Map<RouterKey, K> routerKeyMap;
  private final int longTailRetryMaxRouteForMultiKeyReq;
  private AtomicInteger currentAllowedRetryRouteCnt = new AtomicInteger(0);

  public VeniceMultiKeyPath(
      String storeName,
      int versionNumber,
      String resourceName,
      boolean smartLongTailRetryEnabled,
      int smartLongTailRetryAbortThresholdMs,
      int longTailRetryMaxRouteForMultiKeyReq) {
    // HashMap's performance is better than TreeMap
    this(
        storeName,
        versionNumber,
        resourceName,
        smartLongTailRetryEnabled,
        smartLongTailRetryAbortThresholdMs,
        new HashMap<>(),
        longTailRetryMaxRouteForMultiKeyReq);
  }

  public VeniceMultiKeyPath(
      String storeName,
      int versionNumber,
      String resourceName,
      boolean smartLongTailRetryEnabled,
      int smartLongTailRetryAbortThresholdMs,
      Map<RouterKey, K> routerKeyMap,
      int longTailRetryMaxRouteForMultiKeyReq) {
    super(storeName, versionNumber, resourceName, smartLongTailRetryEnabled, smartLongTailRetryAbortThresholdMs);
    this.keyNum = routerKeyMap.size();
    this.routerKeyMap = routerKeyMap;
    this.longTailRetryMaxRouteForMultiKeyReq = longTailRetryMaxRouteForMultiKeyReq;
  }

  /**
   * Fill the router key map and the index2routerKey map.
   *
   * @param storeName
   * @param resourceName
   * @param keys            Multiple keys from client request; keys have been deserialized to ByteBuffer
   * @param partitionFinder
   * @param maxKeyCount
   * @throws RouterException
   */
  public void initialize(
      String storeName,
      String resourceName,
      Iterable<ByteBuffer> keys,
      VenicePartitionFinder partitionFinder,
      int maxKeyCount,
      RouterStats<AggRouterHttpRequestStats> stats) throws RouterException {
    keyNum = 0;
    int keyIdx = 0;
    int partitionNum;
    VenicePartitioner partitioner;
    try {
      partitionNum = partitionFinder.getNumPartitions(resourceName);
      partitioner = partitionFinder.findPartitioner(getStoreName(), getVersionNumber());
    } catch (VeniceNoHelixResourceException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.of(getStoreName()),
          Optional.of(RequestType.COMPUTE),
          e.getHttpResponseStatus(),
          e.getMessage());
    }

    for (ByteBuffer key: keys) {
      RouterKey routerKey = new RouterKey(key);

      keyNum++;

      if (stats != null) {
        stats.getStatsByType(RequestType.MULTI_GET).recordKeySize(storeName, routerKey.getKeySize());
      }

      // partition lookup
      int partitionId = partitioner.getPartitionId(routerKey.getKeyBuffer(), partitionNum);
      routerKey.setPartitionId(partitionId);
      K routerRequestKey = createRouterRequestKey(key, keyIdx, partitionId);
      this.routerKeyMap.put(routerKey, routerRequestKey);
      ++keyIdx;
    }
    setPartitionKeys(this.routerKeyMap.keySet());

    int keyCount = getPartitionKeys().size();
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
          Optional.of(getStoreName()),
          Optional.of(getRequestType()),
          BAD_REQUEST,
          "Key count in multi-get request should not be zero");
    }
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
          Optional.of(getStoreName()),
          Optional.of(getRequestType()),
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
            Optional.of(getStoreName()),
            Optional.of(getRequestType()),
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
    return this.longTailRetryMaxRouteForMultiKeyReq;
  }

  @Override
  public boolean isLongTailRetryAllowedForNewRoute() {
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
    /**
     * We need to share the {@link #currentAllowedRetryRouteCnt} across all the sub paths.
     */
    this.currentAllowedRetryRouteCnt = ((VeniceMultiKeyPath) originalPath).currentAllowedRetryRouteCnt;
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
