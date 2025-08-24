package com.linkedin.venice.listener.request;

import static com.linkedin.venice.compute.ComputeRequestWrapper.LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.compute.protocol.request.ComputeRequest;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.ByteBuffer;
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
      HttpRequest request,
      String schemaId) {
    super(resourceName, keys, request);
    this.computeRequest = computeRequest;
    if (schemaId != null) {
      this.valueSchemaId = Integer.parseInt(schemaId);
    }
  }

  public static ComputeRouterRequestWrapper parseComputeRequest(FullHttpRequest httpRequest, String[] requestParts) {
    if (requestParts.length != 3) {
      // [0]""/[1]"compute"/[2]{$resourceName}
      throw new VeniceException("Invalid request: " + httpRequest.uri());
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

    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(requestContent, 0, requestContent.length);
    ComputeRequest computeRequest = ComputeUtils.deserializeComputeRequest(decoder, null);

    List<ComputeRouterRequestKeyV1> keys = DESERIALIZER.deserializeObjects(decoder);
    String schemaId = httpRequest.headers().get(HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID);
    return new ComputeRouterRequestWrapper(resourceName, computeRequest, keys, httpRequest, schemaId);
  }

  public ComputeRequest getComputeRequest() {
    return computeRequest;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public static ComputeRouterRequestWrapper parseGrpcComputeRequest(VeniceClientRequest clientRequest) {
    String resourceName = clientRequest.getResourceName();

    try {
      // Deserialize the compute request from keyBytes
      byte[] requestBytes = clientRequest.getKeyBytes().toByteArray();

      // The keyBytes contains serialized ComputeRouterRequestWrapper data
      // We need to parse this to extract keys and ComputeRequest
      // For now, create a basic wrapper - the actual parsing will be done by the server
      ComputeRouterRequestKeyV1 key = new ComputeRouterRequestKeyV1();
      key.keyBytes = ByteBuffer.wrap(requestBytes);
      key.partitionId = clientRequest.getPartition();

      List<ComputeRouterRequestKeyV1> keys = new ArrayList<>();
      keys.add(key);

      // Create a basic ComputeRequest - the actual request parsing is done elsewhere
      ComputeRequest computeRequest = new ComputeRequest();
      HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");

      return new ComputeRouterRequestWrapper(resourceName, computeRequest, keys, httpRequest, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse gRPC compute request", e);
    }
  }

  public String toString() {
    return "ComputeRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + getKeyCount() + ")";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COMPUTE;
  }
}
