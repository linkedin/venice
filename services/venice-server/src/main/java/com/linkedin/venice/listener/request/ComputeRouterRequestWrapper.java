package com.linkedin.venice.listener.request;

import static com.linkedin.venice.compute.ComputeRequestWrapper.LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.compute.protocol.request.ComputeRequest;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.streaming.StreamingUtils;
import com.linkedin.venice.utils.NettyUtils;
import io.netty.handler.codec.http.FullHttpRequest;
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
      String schemaId,
      boolean isRetryRequest,
      boolean isStreamingRequest) {
    super(resourceName, keys, isRetryRequest, isStreamingRequest);
    this.computeRequest = computeRequest;
    if (schemaId != null) {
      this.valueSchemaId = Integer.parseInt(schemaId);
    }
  }

  public static ComputeRouterRequestWrapper parseComputeHttpRequest(
      FullHttpRequest httpRequest,
      String[] requestParts) {
    if (requestParts.length != 3) {
      // [0]""/[1]"compute"/[2]{$resourceName}
      throw new VeniceException("Invalid request: " + httpRequest.uri());
    }
    String resourceName = requestParts[2];
    int apiVersion = parseApiVersion(httpRequest.headers().get(HttpConstants.VENICE_API_VERSION));
    byte[] requestContent = getRequestContent(httpRequest);

    return buildComputeRouterRequestWrapper(
        resourceName,
        apiVersion,
        requestContent,
        httpRequest.headers().get(HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID),
        NettyUtils.containRetryHeader(httpRequest),
        StreamingUtils.isStreamingEnabled(httpRequest));
  }

  // Helper method to handle common logic
  private static ComputeRouterRequestWrapper buildComputeRouterRequestWrapper(
      String resourceName,
      int apiVersion,
      byte[] requestContent,
      String schemaId,
      boolean isRetryRequest,
      boolean isStreamingRequest) {
    validateApiVersion(apiVersion);

    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(requestContent, 0, requestContent.length);

    ComputeRequest computeRequest = ComputeUtils.deserializeComputeRequest(decoder, null);
    List<ComputeRouterRequestKeyV1> keys = DESERIALIZER.deserializeObjects(decoder);

    return new ComputeRouterRequestWrapper(
        resourceName,
        computeRequest,
        keys,
        schemaId,
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
    byte[] requestContent = grpcRequest.getPayload().toByteArray();

    return buildComputeRouterRequestWrapper(
        resourceName,
        apiVersion,
        requestContent,
        Integer.toString(grpcRequest.getComputeValueSchemaId()),
        grpcRequest.getIsRetryRequest(),
        grpcRequest.getIsStreamingRequest());
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
