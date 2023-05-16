package com.linkedin.venice.listener.request;

import static com.linkedin.venice.compute.ComputeRequestWrapper.LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


/**
 * {@code ComputeRouterRequestWrapper} encapsulates a POST request for read-compute from routers.
 */
public class ComputeRouterRequestWrapper extends MultiKeyRouterRequestWrapper<ComputeRouterRequestKeyV1> {
  private final ComputeRequestWrapper computeRequestWrapper;
  private int valueSchemaId = -1;

  private ComputeRouterRequestWrapper(
      String resourceName,
      ComputeRequestWrapper computeRequestWrapper,
      Iterable<ComputeRouterRequestKeyV1> keys,
      HttpRequest request,
      String schemaId) {
    super(resourceName, keys, request);
    this.computeRequestWrapper = computeRequestWrapper;
    if (schemaId != null) {
      this.valueSchemaId = Integer.parseInt(schemaId);
    }
  }

  public static ComputeRouterRequestWrapper parseComputeRequest(FullHttpRequest httpRequest, boolean useFastAvro) {
    URI fullUri = URI.create(httpRequest.uri());
    String path = fullUri.getRawPath();
    String[] requestParts = path.split("/");
    if (requestParts.length != 3) {
      // [0]""/[1]"compute"/[2]{$resourceName}
      throw new VeniceException("Invalid request: " + path);
    }
    String resourceName = requestParts[2];

    // Validate API version
    String apiVersionStr = httpRequest.headers().get(HttpConstants.VENICE_API_VERSION);
    if (apiVersionStr == null) {
      throw new VeniceException("Header: " + HttpConstants.VENICE_API_VERSION + " is missing");
    }
    int apiVersion = Integer.parseInt(apiVersionStr);
    if (apiVersion <= 0 || apiVersion > LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST) {
      throw new VeniceException(
          "Compute API version " + apiVersion + " is invalid. " + "Latest version is "
              + LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST);
    }

    // TODO: xplore the possibility of streaming in the request bytes, and processing it in pipelined fashion
    byte[] requestContent = new byte[httpRequest.content().readableBytes()];
    httpRequest.content().readBytes(requestContent);

    ComputeRequestWrapper computeRequestWrapper = new ComputeRequestWrapper(apiVersion);
    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(requestContent, 0, requestContent.length);
    computeRequestWrapper.deserialize(decoder, useFastAvro);

    Iterable<ComputeRouterRequestKeyV1> keys = parseKeys(decoder);
    String schemaId = httpRequest.headers().get(HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID);
    return new ComputeRouterRequestWrapper(resourceName, computeRequestWrapper, keys, httpRequest, schemaId);
  }

  private static Iterable<ComputeRouterRequestKeyV1> parseKeys(BinaryDecoder decoder) {
    RecordDeserializer<ComputeRouterRequestKeyV1> deserializer =
        FastSerializerDeserializerFactory.getAvroSpecificDeserializer(ComputeRouterRequestKeyV1.class);

    return deserializer.deserializeObjects(decoder);
  }

  public ComputeRequestWrapper getComputeRequest() {
    return computeRequestWrapper;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public String toString() {
    return "ComputeRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + keyCount + ")";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COMPUTE;
  }
}
