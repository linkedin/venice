package com.linkedin.venice.router.api.path;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.log4j.Logger;

import static io.netty.handler.codec.http.HttpResponseStatus.*;


public class VeniceMultiGetPath extends VenicePath {
  private static final Logger LOGGER = Logger.getLogger(VeniceMultiGetPath.class);
  public static final ReadAvroProtocolDefinition EXPECTED_PROTOCOL = ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1;

  private int keyNum;
  private final Map<RouterKey, MultiGetRouterRequestKeyV1> routerKeyMap;
  private final Map<Integer, RouterKey> keyIdxToRouterKey;

  public VeniceMultiGetPath(String resourceName, BasicFullHttpRequest request, VenicePartitionFinder partitionFinder,
      int maxKeyCount, boolean smartLongTailRetryEnabled, int smartLongTailRetryAbortThresholdMs)
      throws RouterException {
    super(resourceName, smartLongTailRetryEnabled, smartLongTailRetryAbortThresholdMs);

    // Validate API version
    int apiVersion = Integer.parseInt(request.headers().get(HttpConstants.VENICE_API_VERSION));
    if (apiVersion != EXPECTED_PROTOCOL.getProtocolVersion()) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          BAD_REQUEST, "Expected api version: " + EXPECTED_PROTOCOL.getProtocolVersion() + ", but received: " + apiVersion);
    }

    // HashMap's performance is better than TreeMap
    this.routerKeyMap = new HashMap<>();
    this.keyIdxToRouterKey = new HashMap<>();
    Iterable<ByteBuffer> keys = null;
    byte[] content = new byte[request.content().readableBytes()];
    request.content().readBytes(content);
    keys = deserialize(content);

    keyNum = 0;
    int keyIdx = 0;
    int partitionNum = partitionFinder.getNumPartitions(resourceName);
    for (ByteBuffer key : keys) {
      RouterKey routerKey = new RouterKey(key);

      keyNum++;
      this.keyIdxToRouterKey.put(keyIdx, routerKey);

      // partition lookup
      int partitionId;
      try {
        partitionId = partitionFinder.findPartitionNumber(routerKey, partitionNum);
        routerKey.setPartitionId(partitionId);
      } catch (VeniceNoHelixResourceException e){
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.of(getStoreName()),
            Optional.of(RequestType.MULTI_GET),
            e.getHttpResponseStatus(),
            e.getMessage());
      }
      MultiGetRouterRequestKeyV1 routerRequestKey = new MultiGetRouterRequestKeyV1();
      routerRequestKey.keyBytes = key;
      routerRequestKey.keyIndex = keyIdx;
      routerRequestKey.partitionId = partitionId;
      this.routerKeyMap.put(routerKey, routerRequestKey);
      ++keyIdx;
    }
    setPartitionKeys(this.routerKeyMap.keySet());

    int keyCount = getPartitionKeys().size();
    if (keyCount > maxKeyCount) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          BAD_REQUEST, "Key count in multi-get request exceeds the threshold: " + maxKeyCount);
    }
    if (0 == keyCount) {
      /**
       * TODO: Right now, there is no good way to return empty response if the key set is empty.
       * The logic to handle empty key here is different from client side, since the client will return empty map
       * instead of throwing an exception.
       *
       * If application is using Venice client to send batch-get request, this piece of logic shouldn't be triggered.
       */
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          BAD_REQUEST, "Key count in multi-get request should not be zero");
    }
  }

  private VeniceMultiGetPath(String resourceName, Map<RouterKey, MultiGetRouterRequestKeyV1> routerKeyMap,
      Map<Integer, RouterKey> keyIdxToRouterKey, int keyNum, boolean smartLongTailRetryEnabled, int smartLongTailRetryAbortThresholdMs) {
    super(resourceName, smartLongTailRetryEnabled, smartLongTailRetryAbortThresholdMs);
    this.routerKeyMap = routerKeyMap;
    this.keyIdxToRouterKey = keyIdxToRouterKey;
    this.keyNum = keyNum;
    setPartitionKeys(routerKeyMap.keySet());
  }

  /**
   * remove a key from the multi-get path;
   * however, don't modify the keyIdxToRouterKey map because we need to maintain a mapping from keyIdx to RouterKey;
   * the MultiGetResponseRecord from the servers only contains keyIdx and doesn't contains the actual key,
   * but we need the key to update the cache.
   * @param key
   */
  public void removeFromRequest(RouterKey key) {
    if (key != null) {
      routerKeyMap.remove(key);
      keyNum--;
    }
  }

  public Set<Map.Entry<Integer, RouterKey>> getKeyIdxToRouterKeySet() { return keyIdxToRouterKey.entrySet(); }

  public RouterKey getRouterKeyByKeyIdx(int keyIdx) { return keyIdxToRouterKey.get(keyIdx); }

  public boolean isEmptyRequest() {
    return 0 == routerKeyMap.size();
  }

  public int getCurrentKeyNum() { return keyNum; }

  @Nonnull
  @Override
  public String getLocation() {
    String sep = VenicePathParser.SEP;
    StringBuilder sb = new StringBuilder();
    sb.append(VenicePathParser.TYPE_STORAGE).append(sep)
        .append(getResourceName());
    return sb.toString();
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.MULTI_GET;
  }

  /**
   * If the parent request is a retry request, the sub-request generated by scattering-gathering logic
   * should be retry request as well.
   * @param subPath
   * @return
   */
  private VeniceMultiGetPath fixRetryRequestForSubPath(VeniceMultiGetPath subPath) {
    subPath.setupRetryRelatedInfo(this);
    return subPath;
  }

  /**
   * The following function is used to generate a {@link VeniceMultiGetPath} for the given key, and the {@link VeniceMultiGetPath}
   * generated will be used to compose router request and forward to storage node;
   *
   * This function will be triggered by DDS router framework.
   *
   * @param s
   * @return
   */
  @Override
  public VenicePath substitutePartitionKey(RouterKey s) {
    MultiGetRouterRequestKeyV1 routerRequestKey = routerKeyMap.get(s);
    if (null == routerRequestKey) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          BAD_GATEWAY, "RouterKey: " + s + " should exist in the original path");
    }
    Map<RouterKey, MultiGetRouterRequestKeyV1> newRouterKeyMap = new HashMap<>();
    Map<Integer, RouterKey> newKeyIdxToRouterKey = new HashMap<>();

    newRouterKeyMap.put(s, routerRequestKey);
    newKeyIdxToRouterKey.put(routerRequestKey.keyIndex, s);

    return fixRetryRequestForSubPath(new VeniceMultiGetPath(getResourceName(), newRouterKeyMap, newKeyIdxToRouterKey,
        1, isSmartLongTailRetryEnabled(), getSmartLongTailRetryAbortThresholdMs()));
  }

  /**
   * The following function is used to generate a {@link VeniceMultiGetPath} for the given key subset, and the {@link VeniceMultiGetPath}
   * generated will be used to compose router request and forward to storage node;
   *
   * This function will be triggered by DDS router framework.
   *
   * @param s
   * @return
   */
  @Override
  public VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s) {
    Map<RouterKey, MultiGetRouterRequestKeyV1> newRouterKeyMap = new HashMap<>();
    Map<Integer, RouterKey> newKeyIdxToRouterKey = new HashMap<>();
    for (RouterKey key : s) {
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
      MultiGetRouterRequestKeyV1 routerRequestKey = routerKeyMap.get(key);
      if (null == routerRequestKey) {
        throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
            BAD_GATEWAY, "RouterKey: " + key + " should exist in the original path");
      }

      newRouterKeyMap.put(key, routerRequestKey);
      newKeyIdxToRouterKey.put(routerRequestKey.keyIndex, key);
    }

    return fixRetryRequestForSubPath(new VeniceMultiGetPath(getResourceName(), newRouterKeyMap, newKeyIdxToRouterKey,
        s.size(), isSmartLongTailRetryEnabled(), getSmartLongTailRetryAbortThresholdMs()));
  }

  @Override
  public HttpUriRequest composeRouterRequest(String storageNodeUri) {
    HttpPost routerRequest = new HttpPost(storageNodeUri + getLocation());
    /**
     * Use {@link ByteArrayEntity} here instead of {@link BasicHttpEntity} to explicitly disable
     * streaming (chunked transfer-encoding) since the streaming might cause some inefficiency
     * in the storage node.
     */
    ByteArrayEntity entity = new ByteArrayEntity(serializedMultiGetRequest());
    routerRequest.setEntity(entity);

    // Setup API version header
    routerRequest.setHeader(HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion()));

    return routerRequest;
  }

  private byte[] serializedMultiGetRequest() {
    RecordSerializer<MultiGetRouterRequestKeyV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.SCHEMA$);

    return serializer.serializeObjects(routerKeyMap.values());
  }

  private static Iterable<ByteBuffer> deserialize(byte[] content) {
    RecordDeserializer<ByteBuffer> deserializer = SerializerDeserializerFactory.getAvroGenericDeserializer(
        EXPECTED_PROTOCOL.getSchema());
    return deserializer.deserializeObjects(
        OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(content, 0, content.length));
  }
}
