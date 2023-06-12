package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


/**
 * {@code MultiGetRouterRequestWrapper} encapsulates a POST request to storage/resourcename on the storage node for a multi-get operation.
 */
public class MultiGetRouterRequestWrapper extends MultiKeyRouterRequestWrapper<MultiGetRouterRequestKeyV1> {
  private MultiGetRouterRequestWrapper(
      String resourceName,
      Iterable<MultiGetRouterRequestKeyV1> keys,
      HttpRequest request) {
    super(resourceName, keys, request);
  }

  public static MultiGetRouterRequestWrapper parseMultiGetHttpRequest(FullHttpRequest httpRequest) {
    URI fullUri = URI.create(httpRequest.uri());
    String path = fullUri.getRawPath();
    String[] requestParts = path.split("/");
    if (requestParts.length != 3) {
      // [0]""/[1]"storage"/[2]{$resourceName}
      throw new VeniceException("Invalid request: " + path);
    }
    String resourceName = requestParts[2];

    // Validate API version
    String apiVersion = httpRequest.headers().get(HttpConstants.VENICE_API_VERSION);
    if (apiVersion == null) {
      throw new VeniceException("Header: " + HttpConstants.VENICE_API_VERSION + " is missing");
    }
    int expectedApiVersion = ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion();
    if (Integer.parseInt(apiVersion) != expectedApiVersion) {
      throw new VeniceException("Expected API version: " + expectedApiVersion + ", but received: " + apiVersion);
    }

    Iterable<MultiGetRouterRequestKeyV1> keys;
    byte[] content = new byte[httpRequest.content().readableBytes()];
    httpRequest.content().readBytes(content);
    keys = parseKeys(content);

    return new MultiGetRouterRequestWrapper(resourceName, keys, httpRequest);
  }

  private static Iterable<MultiGetRouterRequestKeyV1> parseKeys(byte[] content) {
    RecordDeserializer<MultiGetRouterRequestKeyV1> deserializer =
        FastSerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetRouterRequestKeyV1.class);

    return deserializer.deserializeObjects(
        OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(content, 0, content.length));
  }

  public String toString() {
    return "MultiGetRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + keyCount + ")";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.MULTI_GET;
  }
}
