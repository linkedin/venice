package com.linkedin.venice.listener.request;

import static com.linkedin.venice.compute.ComputeRequestWrapper.LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.protocol.request.ComputeAggregationRequest;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.util.List;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


/**
 * {@code ComputeAggregationRouterRequestWrapper} encapsulates a POST request for compute aggregation from routers.
 */
public class ComputeAggregationRouterRequestWrapper extends MultiKeyRouterRequestWrapper<ComputeRouterRequestKeyV1> {
  private static final RecordDeserializer<ComputeRouterRequestKeyV1> DESERIALIZER = FastSerializerDeserializerFactory
      .getFastAvroSpecificDeserializer(ComputeRouterRequestKeyV1.SCHEMA$, ComputeRouterRequestKeyV1.class);

  private final ComputeAggregationRequest aggregationRequest;
  private int valueSchemaId = -1;

  private ComputeAggregationRouterRequestWrapper(
      String resourceName,
      ComputeAggregationRequest aggregationRequest,
      List<ComputeRouterRequestKeyV1> keys,
      HttpRequest request,
      String schemaId) {
    super(resourceName, keys, request);
    this.aggregationRequest = aggregationRequest;
    if (schemaId != null) {
      this.valueSchemaId = Integer.parseInt(schemaId);
    }
  }

  public static ComputeAggregationRouterRequestWrapper parseAggregationRequest(
      FullHttpRequest httpRequest,
      String[] requestParts) {
    if (requestParts.length != 3) {
      // [0]""/[1]"aggregation"/[2]{$resourceName}
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
          "Compute Aggregation API version " + apiVersion + " is invalid. " + "Latest version is "
              + LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST);
    }

    // Deserialize request content
    byte[] requestContent = new byte[httpRequest.content().readableBytes()];
    httpRequest.content().readBytes(requestContent);

    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(requestContent, 0, requestContent.length);

    // TODO: Add proper deserialization for ComputeAggregationRequest
    // For now, we'll create a placeholder
    ComputeAggregationRequest aggregationRequest = new ComputeAggregationRequest();

    List<ComputeRouterRequestKeyV1> keys = DESERIALIZER.deserializeObjects(decoder);
    String schemaId = httpRequest.headers().get(HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID);
    return new ComputeAggregationRouterRequestWrapper(resourceName, aggregationRequest, keys, httpRequest, schemaId);
  }

  public ComputeAggregationRequest getAggregationRequest() {
    return aggregationRequest;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public String toString() {
    return "ComputeAggregationRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + getKeyCount()
        + ")";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COMPUTE_AGGREGATION;
  }
}
