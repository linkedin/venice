package com.linkedin.venice.listener.request;

import static com.linkedin.venice.compute.ComputeRequestWrapper.LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.compute.protocol.request.ComputeRequest;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.MultiKeyRequestKey;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.streaming.StreamingUtils;
import com.linkedin.venice.utils.NettyUtils;
import io.netty.handler.codec.http.FullHttpRequest;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


/**
 * {@code ComputeRouterRequestWrapper} encapsulates a POST request for read-compute from routers.
 */
public class ComputeRouterRequestWrapper extends MultiKeyRouterRequestWrapper<ComputeRouterRequestKeyV1> {
  private static final RecordDeserializer<ComputeRouterRequestKeyV1> DESERIALIZER = FastSerializerDeserializerFactory
      .getFastAvroSpecificDeserializer(ComputeRouterRequestKeyV1.SCHEMA$, ComputeRouterRequestKeyV1.class);

  private final ComputeRequest computeRequest;
  private int valueSchemaId = -1;

  private ComputeRouterRequestWrapper(
      String resourceName,
      ComputeRequest computeRequest,
      List<ComputeRouterRequestKeyV1> keys,
      int schemaId,
      boolean isRetryRequest,
      boolean isStreamingRequest) {
    super(resourceName, keys, isRetryRequest, isStreamingRequest);
    this.computeRequest = computeRequest;
    this.valueSchemaId = schemaId;
  }

  public static ComputeRouterRequestWrapper parseComputeHttpRequest(
      FullHttpRequest httpRequest,
      String[] requestParts) {
    if (requestParts.length != 3) {
      // [0]""/[1]"compute"/[2]{$resourceName}
      throw new VeniceException("Invalid request: " + httpRequest.uri());
    }
    int apiVersion = parseApiVersion(httpRequest.headers().get(HttpConstants.VENICE_API_VERSION));
    validateApiVersion(apiVersion);
    String resourceName = requestParts[2];
    byte[] requestContent = getRequestContent(httpRequest);

    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(requestContent, 0, requestContent.length);
    ComputeRequest computeRequest = ComputeUtils.deserializeComputeRequest(decoder, null);
    List<ComputeRouterRequestKeyV1> keys = DESERIALIZER.deserializeObjects(decoder);

    boolean isRetryRequest = NettyUtils.containRetryHeader(httpRequest);
    boolean isStreamingRequest = StreamingUtils.isStreamingEnabled(httpRequest);
    String schemaId = httpRequest.headers().get(HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID);
    int valueSchemaId = schemaId == null ? -1 : Integer.parseInt(schemaId);

    return new ComputeRouterRequestWrapper(
        resourceName,
        computeRequest,
        keys,
        valueSchemaId,
        isRetryRequest,
        isStreamingRequest);
  }

  // Extracted version validation logic
  private static int parseApiVersion(String apiVersionStr) {
    if (apiVersionStr == null) {
      throw new VeniceException("Header: " + HttpConstants.VENICE_API_VERSION + " is missing");
    }

    return Integer.parseInt(apiVersionStr);
  }

  // Extracted API version validation method
  private static void validateApiVersion(int apiVersion) {
    if (apiVersion <= 0 || apiVersion > LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST) {
      throw new VeniceException(
          "Compute API version " + apiVersion + " is invalid. Latest version is "
              + LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST);
    }
  }

  // Helper to extract content from HTTP request
  private static byte[] getRequestContent(FullHttpRequest httpRequest) {
    byte[] requestContent = new byte[httpRequest.content().readableBytes()];
    httpRequest.content().readBytes(requestContent);
    return requestContent;
  }

  public static ComputeRouterRequestWrapper parseComputeGrpcRequest(
      com.linkedin.venice.protocols.ComputeRequest grpcRequest) {
    String resourceName = grpcRequest.getResourceName();
    int apiVersion = grpcRequest.getApiVersion();
    byte[] computeRequestBytes = grpcRequest.getComputeRequestBytes().toByteArray();
    validateApiVersion(apiVersion);
    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(computeRequestBytes, 0, computeRequestBytes.length);
    ComputeRequest computeRequest = ComputeUtils.deserializeComputeRequest(decoder, null);

    int computeRequestSchemaId = grpcRequest.getComputeValueSchemaId();
    boolean isRetryRequest = grpcRequest.getIsRetryRequest();

    List<ComputeRouterRequestKeyV1> keys = new ArrayList<>(grpcRequest.getKeyCount());
    for (int i = 0; i < grpcRequest.getKeyCount(); i++) {
      MultiKeyRequestKey multiKeyRequestKey = grpcRequest.getKeys(i);
      keys.add(
          new ComputeRouterRequestKeyV1(
              multiKeyRequestKey.getKeyIndex(),
              multiKeyRequestKey.getKeyBytes().asReadOnlyByteBuffer(),
              multiKeyRequestKey.getPartition()));
    }

    return new ComputeRouterRequestWrapper(
        resourceName,
        computeRequest,
        keys,
        computeRequestSchemaId,
        isRetryRequest,
        true);
  }

  public ComputeRequest getComputeRequest() {
    return computeRequest;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public String toString() {
    return "ComputeRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + getKeyCount() + ")";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COMPUTE;
  }
}
