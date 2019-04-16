package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV1;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.schema.avro.ComputableSerializerDeserializerFactory;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


public class ComputeRouterRequestWrapper extends MultiKeyRouterRequestWrapper<ComputeRouterRequestKeyV1>{
  private final ComputeRequestV1 computeRequest;

  private ComputeRouterRequestWrapper(String resourceName, ComputeRequestV1 computeRequest,
                                      Iterable<ComputeRouterRequestKeyV1> keys, HttpRequest request) {
    super(resourceName, keys, request);
    this.computeRequest = computeRequest;
  }

  public static ComputeRouterRequestWrapper parseComputeRequest(FullHttpRequest httpRequest, boolean useFastAvro) {
    URI fullUri = URI.create(httpRequest.uri());
    String path = fullUri.getRawPath();
    String[] requestParts = path.split("/");
    if (requestParts.length != 3) { // [0]""/[1]"compute"/[2]{$resourceName}
      throw new VeniceException("Invalid request: " + path);
    }
    String resourceName = requestParts[2];

    // Validate API version
    String apiVersion = httpRequest.headers().get(HttpConstants.VENICE_API_VERSION);
    if (null == apiVersion) {
      throw new VeniceException("Header: " + HttpConstants.VENICE_API_VERSION + " is missing");
    }
    int expectedApiVersion = ReadAvroProtocolDefinition.COMPUTE_ROUTER_REQUEST_V1.getProtocolVersion();
    if (Integer.parseInt(apiVersion) != expectedApiVersion) {
      throw new VeniceException("Expected API version: " + expectedApiVersion + ", but received: " + apiVersion);
    }

    // TODO: xplore the possibility of streaming in the request bytes, and processing it in pipelined fashion
    byte[] requestContent = new byte[httpRequest.content().readableBytes()];
    httpRequest.content().readBytes(requestContent);

    RecordDeserializer<ComputeRequestV1> recordDeserializer;
    if (useFastAvro) {
      recordDeserializer = FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(ComputeRequestV1.SCHEMA$, ComputeRequestV1.class);
    } else {
      recordDeserializer = ComputableSerializerDeserializerFactory.getComputableAvroSpecificDeserializer(ComputeRequestV1.SCHEMA$,
          ComputeRequestV1.class);
    }
    BinaryDecoder decoder =
        OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(requestContent, 0, requestContent.length);
    ComputeRequestV1 computeRequest = recordDeserializer.deserialize(decoder);

    Iterable<ComputeRouterRequestKeyV1> keys = parseKeys(decoder);

    return new ComputeRouterRequestWrapper(resourceName, computeRequest, keys, httpRequest);
  }

  private static Iterable<ComputeRouterRequestKeyV1> parseKeys(BinaryDecoder decoder) {
    RecordDeserializer<ComputeRouterRequestKeyV1> deserializer =
        SerializerDeserializerFactory.getAvroSpecificDeserializer(ComputeRouterRequestKeyV1.class);

    return deserializer.deserializeObjects(decoder);
  }

  public ComputeRequestV1 getComputeRequest() {
    return computeRequest;
  }

  public String toString() {
    return "ComputeRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + keyCount + ")";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COMPUTE;
  }
}
