package com.linkedin.venice.router.api.path;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;

import static com.linkedin.venice.compute.ComputeRequestWrapper.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class VeniceComputePath extends VeniceMultiKeyPath<ComputeRouterRequestKeyV1> {
  // Compute request is useless for now in router, until we support ranking in the future.
  private final ComputeRequestWrapper computeRequestWrapper;
  private final byte[] requestContent;
  private final int computeRequestLengthInBytes;

  private final int computeRequestVersion;

  public VeniceComputePath(String resourceName, BasicFullHttpRequest request, VenicePartitionFinder partitionFinder,
      int maxKeyCount, boolean smartLongTailRetryEnabled, int smartLongTailRetryAbortThresholdMs, boolean useFastAvro)
      throws RouterException {
    super(resourceName, smartLongTailRetryEnabled, smartLongTailRetryAbortThresholdMs);

    // Get API version
    computeRequestVersion = Integer.parseInt(request.headers().get(HttpConstants.VENICE_API_VERSION));
    if (computeRequestVersion <= 0 || computeRequestVersion > LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()), BAD_REQUEST,
          "Compute API version " + computeRequestVersion + " is invalid. Latest version is " + LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST);
    }

    Iterable<ByteBuffer> keys = null;
    requestContent = new byte[request.content().readableBytes()];
    request.content().readBytes(requestContent);

    /**
     * The first part of the request content from client is the ComputeRequest which contains an array of operations
     * and the result schema string for now; deserialize the first part and record the length of the first part
     */
    computeRequestWrapper = new ComputeRequestWrapper(computeRequestVersion);
    BinaryDecoder decoder =
        OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(requestContent, 0, requestContent.length);
    computeRequestWrapper.deserialize(decoder, useFastAvro);
    try {
      // record the length of the serialized ComputeRequest
      computeRequestLengthInBytes = requestContent.length - decoder.inputStream().available();
    } catch (IOException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          BAD_REQUEST,"Exception while getting available number of bytes in request content");
    }

    // deserialize the second part of the request content using the same decoder
    RecordDeserializer<ByteBuffer> keyDeserializer = SerializerDeserializerFactory.getAvroGenericDeserializer(
        ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema());
    keys = keyDeserializer.deserializeObjects(decoder);

    initialize(resourceName, keys, partitionFinder, maxKeyCount);
  }

  private VeniceComputePath(String resourceName, Map<RouterKey, ComputeRouterRequestKeyV1> routerKeyMap,
      Map<Integer, RouterKey> keyIdxToRouterKey, ComputeRequestWrapper computeRequestWrapper, byte[] requestContent,
      int computeRequestLengthInBytes, int computeRequestVersion, boolean smartLongTailRetryEnabled,
      int smartLongTailRetryAbortThresholdMs) {
    super(resourceName, smartLongTailRetryEnabled, smartLongTailRetryAbortThresholdMs, routerKeyMap, keyIdxToRouterKey);
    this.computeRequestWrapper = computeRequestWrapper;
    this.requestContent = requestContent;
    this.computeRequestLengthInBytes = computeRequestLengthInBytes;
    this.computeRequestVersion = computeRequestVersion;
    setPartitionKeys(routerKeyMap.keySet());
  }

  @Nonnull
  @Override
  public String getLocation() {
    StringBuilder sb = new StringBuilder();
    sb.append(VenicePathParser.TYPE_COMPUTE)
        .append(VenicePathParser.SEP)
        .append(getResourceName());
    return sb.toString();
  }

  @Override
  public RequestType getRequestType() {
    return isStreamingRequest() ? RequestType.COMPUTE_STREAMING : RequestType.COMPUTE;
  }

  /**
   * If the parent request is a retry request, the sub-request generated by scattering-gathering logic
   * should be retry request as well.
   *
   * @param routerKeyMap
   * @param keyIdxToRouterKey
   * @return
   */
  @Override
  protected VeniceComputePath fixRetryRequestForSubPath(Map<RouterKey, ComputeRouterRequestKeyV1> routerKeyMap,
      Map<Integer, RouterKey> keyIdxToRouterKey) {
    VeniceComputePath subPath = new VeniceComputePath(getResourceName(), routerKeyMap, keyIdxToRouterKey,
        this.computeRequestWrapper, this.requestContent, this.computeRequestLengthInBytes, this.computeRequestVersion,
        isSmartLongTailRetryEnabled(), getSmartLongTailRetryAbortThresholdMs());
    subPath.setupRetryRelatedInfo(this);
    return subPath;
  }

  @Override
  protected ComputeRouterRequestKeyV1 createRouterRequestKey(ByteBuffer key, int keyIdx, int partitionId) {
    ComputeRouterRequestKeyV1 routerRequestKey = new ComputeRouterRequestKeyV1();
    routerRequestKey.keyBytes = key;
    routerRequestKey.keyIndex = keyIdx;
    routerRequestKey.partitionId = partitionId;
    return routerRequestKey;
  }

  @Override
  protected int getKeyIndex(ComputeRouterRequestKeyV1 routerRequestKey) {
    return routerRequestKey.keyIndex;
  }

  @Override
  protected byte[] serializeRouterRequest() {
    RecordSerializer<ComputeRouterRequestKeyV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeRouterRequestKeyV1.SCHEMA$);

    return serializer.serializeObjects(
        routerKeyMap.values(),
        ByteBuffer.wrap(requestContent, 0, computeRequestLengthInBytes)
    );
  }

  @Override
  public String getVeniceApiVersionHeader() {
    return String.valueOf(computeRequestVersion);
  }

  // for testing
  protected ComputeRequestWrapper getComputeRequest() {
    return computeRequestWrapper;
  }

  // for testing
  protected int getComputeRequestLengthInBytes() {
    return computeRequestLengthInBytes;
  }
}
