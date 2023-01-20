package com.linkedin.venice.router.api.path;

import static com.linkedin.venice.router.api.VenicePathParser.TYPE_STORAGE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.router.RouterThrottleHandler;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


public class VeniceMultiGetPath extends VeniceMultiKeyPath<MultiGetRouterRequestKeyV1> {
  private static final String ROUTER_REQUEST_VERSION =
      Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion());

  protected static final ReadAvroProtocolDefinition EXPECTED_PROTOCOL =
      ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1;

  public VeniceMultiGetPath(
      String resourceName,
      BasicFullHttpRequest request,
      VenicePartitionFinder partitionFinder,
      int maxKeyCount,
      boolean smartLongTailRetryEnabled,
      int smartLongTailRetryAbortThresholdMs,
      int longTailRetryMaxRouteForMultiKeyReq) throws RouterException {
    this(
        resourceName,
        request,
        partitionFinder,
        maxKeyCount,
        smartLongTailRetryEnabled,
        smartLongTailRetryAbortThresholdMs,
        Optional.empty(),
        longTailRetryMaxRouteForMultiKeyReq);
  }

  public VeniceMultiGetPath(
      String resourceName,
      BasicFullHttpRequest request,
      VenicePartitionFinder partitionFinder,
      int maxKeyCount,
      boolean smartLongTailRetryEnabled,
      int smartLongTailRetryAbortThresholdMs,
      Optional<RouterStats<AggRouterHttpRequestStats>> stats,
      int longTailRetryMaxRouteForMultiKeyReq) throws RouterException {
    super(
        resourceName,
        smartLongTailRetryEnabled,
        smartLongTailRetryAbortThresholdMs,
        longTailRetryMaxRouteForMultiKeyReq);

    // Validate API version
    int apiVersion = Integer.parseInt(request.headers().get(HttpConstants.VENICE_API_VERSION));
    if (apiVersion != EXPECTED_PROTOCOL.getProtocolVersion()) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.of(getStoreName()),
          Optional.of(getRequestType()),
          BAD_REQUEST,
          "Expected api version: " + EXPECTED_PROTOCOL.getProtocolVersion() + ", but received: " + apiVersion);
    }

    Iterable<ByteBuffer> keys;
    byte[] content;

    if (request.hasAttr(RouterThrottleHandler.THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY)) {
      content = request.attr(RouterThrottleHandler.THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY).get();
    } else {
      content = new byte[request.content().readableBytes()];
      request.content().readBytes(content);
    }

    keys = deserialize(content);
    initialize(resourceName, keys, partitionFinder, maxKeyCount, stats);
  }

  private VeniceMultiGetPath(
      String resourceName,
      Map<RouterKey, MultiGetRouterRequestKeyV1> routerKeyMap,
      Map<Integer, RouterKey> keyIdxToRouterKey,
      boolean smartLongTailRetryEnabled,
      int smartLongTailRetryAbortThresholdMs,
      int longTailRetryMaxRouteForMultiKeyReq) {
    super(
        resourceName,
        smartLongTailRetryEnabled,
        smartLongTailRetryAbortThresholdMs,
        routerKeyMap,
        keyIdxToRouterKey,
        longTailRetryMaxRouteForMultiKeyReq);
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

  public Set<Map.Entry<Integer, RouterKey>> getKeyIdxToRouterKeySet() {
    return keyIdxToRouterKey.entrySet();
  }

  public RouterKey getRouterKeyByKeyIdx(int keyIdx) {
    return keyIdxToRouterKey.get(keyIdx);
  }

  public boolean isEmptyRequest() {
    return routerKeyMap.isEmpty();
  }

  public int getCurrentKeyNum() {
    return keyNum;
  }

  @Nonnull
  @Override
  public String getLocation() {
    StringBuilder sb = new StringBuilder();
    sb.append(TYPE_STORAGE).append(VenicePathParser.SEP).append(getResourceName());
    return sb.toString();
  }

  @Override
  public final RequestType getRequestType() {
    return isStreamingRequest() ? RequestType.MULTI_GET_STREAMING : RequestType.MULTI_GET;
  }

  /**
   * If the parent request is a retry request, the sub-request generated by scattering-gathering logic
   * should be retry request as well.
   *
   * @param routerKeyMap
   * @param keyIdxToRouterKey
   * @return
   */
  protected VeniceMultiGetPath fixRetryRequestForSubPath(
      Map<RouterKey, MultiGetRouterRequestKeyV1> routerKeyMap,
      Map<Integer, RouterKey> keyIdxToRouterKey) {
    VeniceMultiGetPath subPath = new VeniceMultiGetPath(
        getResourceName(),
        routerKeyMap,
        keyIdxToRouterKey,
        isSmartLongTailRetryEnabled(),
        getSmartLongTailRetryAbortThresholdMs(),
        getLongTailRetryMaxRouteForMultiKeyReq());
    subPath.setupRetryRelatedInfo(this);
    return subPath;
  }

  @Override
  protected MultiGetRouterRequestKeyV1 createRouterRequestKey(ByteBuffer key, int keyIdx, int partitionId) {
    MultiGetRouterRequestKeyV1 routerRequestKey = new MultiGetRouterRequestKeyV1();
    routerRequestKey.keyBytes = key;
    routerRequestKey.keyIndex = keyIdx;
    routerRequestKey.partitionId = partitionId;
    return routerRequestKey;
  }

  @Override
  protected int getKeyIndex(MultiGetRouterRequestKeyV1 routerRequestKey) {
    return routerRequestKey.keyIndex;
  }

  @Override
  protected byte[] serializeRouterRequest() {
    RecordSerializer<MultiGetRouterRequestKeyV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.getClassSchema());

    return serializer.serializeObjects(routerKeyMap.values(), AvroSerializer.REUSE.get());
  }

  @Override
  public void setRestRequestEntity(RestRequestBuilder builder) {
    builder.setEntity(serializeRouterRequest());
  }

  private static Iterable<ByteBuffer> deserialize(byte[] content) {
    RecordDeserializer<ByteBuffer> deserializer =
        SerializerDeserializerFactory.getAvroGenericDeserializer(EXPECTED_PROTOCOL.getSchema());
    return deserializer.deserializeObjects(
        OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(content, 0, content.length));
  }

  @Override
  public String getVeniceApiVersionHeader() {
    return ROUTER_REQUEST_VERSION;
  }
}
