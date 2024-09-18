package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.MultiGetRequest;
import com.linkedin.venice.protocols.MultiKeyRequestKey;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.streaming.StreamingUtils;
import com.linkedin.venice.utils.NettyUtils;
import io.netty.handler.codec.http.FullHttpRequest;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


/**
 * {@code MultiGetRouterRequestWrapper} encapsulates a POST request to storage/resourcename on the storage node for a multi-get operation.
 */
public class MultiGetRouterRequestWrapper extends MultiKeyRouterRequestWrapper<MultiGetRouterRequestKeyV1> {
  private static final RecordDeserializer<MultiGetRouterRequestKeyV1> DESERIALIZER = FastSerializerDeserializerFactory
      .getFastAvroSpecificDeserializer(MultiGetRouterRequestKeyV1.SCHEMA$, MultiGetRouterRequestKeyV1.class);

  private MultiGetRouterRequestWrapper(
      String resourceName,
      List<MultiGetRouterRequestKeyV1> keys,
      boolean isRetryRequest,
      boolean isStreamingRequest) {
    super(resourceName, keys, isRetryRequest, isStreamingRequest);
  }

  public static MultiGetRouterRequestWrapper parseMultiGetHttpRequest(
      FullHttpRequest httpRequest,
      String[] requestParts) {
    if (requestParts.length != 3) {
      // [0]""/[1]"storage"/[2]{$resourceName}
      throw new VeniceException("Invalid request: " + httpRequest.uri());
    }
    // Validate API version
    String apiVersion = httpRequest.headers().get(HttpConstants.VENICE_API_VERSION);
    if (apiVersion == null) {
      throw new VeniceException("Header: " + HttpConstants.VENICE_API_VERSION + " is missing");
    }
    int expectedApiVersion = ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion();
    if (Integer.parseInt(apiVersion) != expectedApiVersion) {
      throw new VeniceException("Expected API version: " + expectedApiVersion + ", but received: " + apiVersion);
    }

    List<MultiGetRouterRequestKeyV1> keys;
    byte[] content = new byte[httpRequest.content().readableBytes()];
    httpRequest.content().readBytes(content);
    keys = parseKeys(content);
    boolean isRetryRequest = NettyUtils.containRetryHeader(httpRequest);
    boolean isStreamingRequest = StreamingUtils.isStreamingEnabled(httpRequest);

    return new MultiGetRouterRequestWrapper(requestParts[2], keys, isRetryRequest, isStreamingRequest);
  }

  /**
   * @deprecated This method has been deprecated and will be removed once the corresponding legacy gRPC code is removed.
   */
  @Deprecated
  public static MultiGetRouterRequestWrapper parseMultiGetGrpcRequest(VeniceClientRequest grpcRequest) {
    String resourceName = grpcRequest.getResourceName();
    List<MultiGetRouterRequestKeyV1> keys = parseKeys(grpcRequest.getKeyBytes().toByteArray());
    boolean isRetryRequest = grpcRequest.getIsRetryRequest();
    boolean isStreamingRequest = grpcRequest.getIsStreamingRequest();
    return new MultiGetRouterRequestWrapper(resourceName, keys, isRetryRequest, isStreamingRequest);
  }

  // TODO: Get rid of the avro envelope and use something generic
  public static MultiGetRouterRequestWrapper parseMultiGetGrpcRequest(MultiGetRequest grpcRequest) {
    String resourceName = grpcRequest.getResourceName();
    List<MultiGetRouterRequestKeyV1> keys = new ArrayList<>(grpcRequest.getKeyCount());
    for (int i = 0; i < grpcRequest.getKeyCount(); i++) {
      MultiKeyRequestKey multiKeyRequestKey = grpcRequest.getKeys(i);
      keys.add(
          new MultiGetRouterRequestKeyV1(
              multiKeyRequestKey.getKeyIndex(),
              multiKeyRequestKey.getKeyBytes().asReadOnlyByteBuffer(),
              multiKeyRequestKey.getPartition()));
    }
    boolean isRetryRequest = grpcRequest.getIsRetryRequest();
    return new MultiGetRouterRequestWrapper(resourceName, keys, isRetryRequest, true);
  }

  private static List<MultiGetRouterRequestKeyV1> parseKeys(byte[] content) {
    return DESERIALIZER.deserializeObjects(
        OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(content, 0, content.length));
  }

  public String toString() {
    return "MultiGetRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + getKeyCount() + ")";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.MULTI_GET;
  }
}
