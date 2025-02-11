package com.linkedin.venice.router.api.path;

import static com.linkedin.venice.HttpConstants.VENICE_CLIENT_COMPUTE_TRUE;
import static com.linkedin.venice.HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID;
import static com.linkedin.venice.compute.ComputeRequestWrapper.LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_COMPUTE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV3;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.meta.StoreVersionName;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.router.RouterRetryConfig;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VeniceResponseDecompressor;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


public class VeniceComputePath extends VeniceMultiKeyPath<ComputeRouterRequestKeyV1> {
  private static final Schema EMPTY_RECORD_SCHEMA = Schema.createRecord(
      ComputeRequestV3.class.getSimpleName(),
      "no-op",
      ComputeRequestV3.class.getPackage().getName(),
      false,
      Collections.emptyList());
  private static final ThreadLocal<GenericRecord> EMPTY_COMPUTE_REQUEST_RECORD =
      ThreadLocal.withInitial(() -> new GenericData.Record(EMPTY_RECORD_SCHEMA));

  /**
   * N.B. This deserializer takes V3 as the writer schema, but the reader schema is just an empty record.
   *
   * There are a few important details here:
   *
   * 1. The router need not actually read the compute request, but rather merely skip over it. That is why we use an
   *    empty record. It cannot be any empty record though, it has to be one with the same FQCN, which is why we build
   *    it the way we do in {@link #EMPTY_RECORD_SCHEMA}.
   *
   * 2. Historically, we've had three versions of the compute request used over the wire (V1 through V3), but in fact,
   *    V3 is capable of deserializing the previous two as well. This is because these schemas have only ever added new
   *    branches to the {@link com.linkedin.venice.compute.protocol.request.ComputeRequest#operations} union, and thus
   *    the schema with all the branches can deserialize those with fewer branches. For this reason, it is not necessary
   *    here to take the schema the client used to encode as the writer schema the router uses to decode. If, however,
   *    in the future, we keep evolving the compute request protocol, we need to reevaluate if the evolution will
   *    require passing in the precise writer schema used. For example, if adding a new field, we would need to start
   *    using the correct writer schema (either V3 or the newer one).
   */
  private static final RecordDeserializer<GenericRecord> COMPUTE_REQUEST_NO_OP_DESERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(ComputeRequestV3.SCHEMA$, EMPTY_RECORD_SCHEMA);
  private static final RecordDeserializer<ByteBuffer> COMPUTE_REQUEST_CLIENT_KEY_V1_DESERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(
          ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema(),
          ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema());
  private static final RecordSerializer<ComputeRouterRequestKeyV1> COMPUTE_ROUTER_REQUEST_KEY_V1_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(ComputeRouterRequestKeyV1.getClassSchema());

  private static void skipOverComputeRequest(BinaryDecoder decoder) {
    COMPUTE_REQUEST_NO_OP_DESERIALIZER.deserialize(EMPTY_COMPUTE_REQUEST_RECORD.get(), decoder);
  }

  private final byte[] requestContent;
  private final int computeRequestLengthInBytes;
  private final String valueSchemaIdHeader;
  private final String computeRequestVersionHeader;

  public VeniceComputePath(
      StoreVersionName storeVersionName,
      BasicFullHttpRequest request,
      VenicePartitionFinder partitionFinder,
      int maxKeyCount,
      AggRouterHttpRequestStats stats,
      RouterRetryConfig retryConfig,
      RetryManager retryManager,
      VeniceResponseDecompressor responseDecompressor) throws RouterException {
    super(storeVersionName, retryConfig, retryManager, responseDecompressor);

    this.valueSchemaIdHeader = request.headers().get(VENICE_COMPUTE_VALUE_SCHEMA_ID, "-1");

    // Get API version
    this.computeRequestVersionHeader = request.headers().get(HttpConstants.VENICE_API_VERSION);
    int computeRequestVersion = Integer.parseInt(this.computeRequestVersionHeader);
    if (computeRequestVersion <= 0 || computeRequestVersion > LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          getStoreName(),
          getRequestType(),
          BAD_REQUEST,
          "Compute API version " + computeRequestVersion + " is invalid. Latest version is "
              + LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST);
    }

    requestContent = new byte[request.content().readableBytes()];
    request.content().readBytes(requestContent);

    /**
     * The first part of the request content from client is the ComputeRequest which contains an array of operations
     * and the result schema string. Here, we deserialize the first part (but throw it away, as it is only to advance
     * the internal state of the decoder) and record the length of the first part.
     */
    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(requestContent, 0, requestContent.length);
    skipOverComputeRequest(decoder);
    try {
      // record the length of the serialized ComputeRequest
      computeRequestLengthInBytes = requestContent.length - decoder.inputStream().available();
    } catch (IOException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          getStoreName(),
          getRequestType(),
          BAD_REQUEST,
          "Exception while getting available number of bytes in request content");
    }

    // deserialize the second part of the request content using the same decoder
    List<ByteBuffer> keys = COMPUTE_REQUEST_CLIENT_KEY_V1_DESERIALIZER.deserializeObjects(decoder);

    initialize(storeVersionName.getName(), keys, partitionFinder, maxKeyCount, stats);
  }

  private VeniceComputePath(
      StoreVersionName storeVersionName,
      Map<RouterKey, ComputeRouterRequestKeyV1> routerKeyMap,
      byte[] requestContent,
      int computeRequestLengthInBytes,
      String valueSchemaIdHeader,
      String computeRequestVersionHeader,
      RouterRetryConfig retryConfig,
      RetryManager retryManager,
      VeniceResponseDecompressor responseDecompressor) {
    super(storeVersionName, routerKeyMap, retryConfig, retryManager, responseDecompressor);
    this.requestContent = requestContent;
    this.valueSchemaIdHeader = valueSchemaIdHeader;
    this.computeRequestLengthInBytes = computeRequestLengthInBytes;
    this.computeRequestVersionHeader = computeRequestVersionHeader;
    setPartitionKeys(routerKeyMap.keySet());
  }

  @Nonnull
  @Override
  public String getLocation() {
    return TYPE_COMPUTE + VenicePathParser.SEP + getResourceName();
  }

  @Override
  public final RequestType getRequestType() {
    return isStreamingRequest() ? getStreamingRequestType() : RequestType.COMPUTE;
  }

  @Override
  public final RequestType getStreamingRequestType() {
    return RequestType.COMPUTE_STREAMING;
  }

  public VeniceMultiGetPath toMultiGetPath() {
    Map<RouterKey, MultiGetRouterRequestKeyV1> newRouterKeyMap = new HashMap<>();
    for (Map.Entry<RouterKey, ComputeRouterRequestKeyV1> entry: routerKeyMap.entrySet()) {
      ComputeRouterRequestKeyV1 computeRequestKey = entry.getValue();
      newRouterKeyMap.put(
          entry.getKey(),
          new MultiGetRouterRequestKeyV1(
              computeRequestKey.getKeyIndex(),
              computeRequestKey.getKeyBytes(),
              computeRequestKey.getPartitionId()));
    }
    VeniceMultiGetPath newPath = new VeniceMultiGetPath(
        storeVersionName,
        newRouterKeyMap,
        this.retryConfig,
        retryManager,
        getResponseDecompressor(),
        VENICE_CLIENT_COMPUTE_TRUE);
    newPath.setupRetryRelatedInfo(this);
    return newPath;
  }

  /**
   * If the parent request is a retry request, the sub-request generated by scattering-gathering logic should be retry
   * request as well.
   *
   * @param routerKeyMap
   * @return
   */
  @Override
  protected VeniceComputePath fixRetryRequestForSubPath(Map<RouterKey, ComputeRouterRequestKeyV1> routerKeyMap) {
    VeniceComputePath subPath = new VeniceComputePath(
        this.storeVersionName,
        routerKeyMap,
        this.requestContent,
        this.computeRequestLengthInBytes,
        this.valueSchemaIdHeader,
        this.computeRequestVersionHeader,
        this.retryConfig,
        this.retryManager,
        getResponseDecompressor());
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
  protected byte[] serializeRouterRequest() {
    return COMPUTE_ROUTER_REQUEST_KEY_V1_SERIALIZER
        .serializeObjects(routerKeyMap.values(), ByteBuffer.wrap(requestContent, 0, computeRequestLengthInBytes));
  }

  @Override
  public void setupVeniceHeaders(BiConsumer<String, String> setupHeaderFunc) {
    super.setupVeniceHeaders(setupHeaderFunc);
    setupHeaderFunc.accept(VENICE_COMPUTE_VALUE_SCHEMA_ID, this.valueSchemaIdHeader);
  }

  @Override
  public String getVeniceApiVersionHeader() {
    return computeRequestVersionHeader;
  }

  // for testing
  protected int getComputeRequestLengthInBytes() {
    return computeRequestLengthInBytes;
  }
}
