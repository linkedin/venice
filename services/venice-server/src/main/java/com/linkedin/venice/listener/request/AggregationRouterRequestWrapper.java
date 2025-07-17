package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


/**
 * AggregationRouterRequestWrapper encapsulates a POST request for aggregation (e.g. countByValue) from routers.
 */
public class AggregationRouterRequestWrapper extends MultiKeyRouterRequestWrapper<ComputeRouterRequestKeyV1> {
  private static final RecordDeserializer<ComputeRouterRequestKeyV1> DESERIALIZER = FastSerializerDeserializerFactory
      .getFastAvroSpecificDeserializer(ComputeRouterRequestKeyV1.SCHEMA$, ComputeRouterRequestKeyV1.class);

  // For now, only support countByValue. Map<fieldName, topK>
  private final Map<String, Integer> countByValueFields;
  private int valueSchemaId = -1;

  private AggregationRouterRequestWrapper(
      String resourceName,
      Map<String, Integer> countByValueFields,
      List<ComputeRouterRequestKeyV1> keys,
      HttpRequest request,
      String schemaId) {
    super(resourceName, keys, request);
    this.countByValueFields = countByValueFields;
    if (schemaId != null) {
      this.valueSchemaId = Integer.parseInt(schemaId);
    }
  }

  public static AggregationRouterRequestWrapper parseAggregationRequest(
      FullHttpRequest httpRequest,
      String[] requestParts) {
    if (requestParts.length != 3) {
      throw new VeniceException("Invalid aggregation request: " + httpRequest.uri());
    }
    String resourceName = requestParts[2];

    // Validate API version
    String apiVersionStr = httpRequest.headers().get(HttpConstants.VENICE_API_VERSION);
    if (apiVersionStr == null) {
      throw new VeniceException("Header: " + HttpConstants.VENICE_API_VERSION + " is missing");
    }
    // API version validation can be added here if needed

    // Read request body
    byte[] requestContent = new byte[httpRequest.content().readableBytes()];
    httpRequest.content().readBytes(requestContent);
    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory()
        .createOptimizedBinaryDecoder(requestContent, 0, requestContent.length);

    // Assume request body first part is Map<String, Integer> countByValueFields, second part is keys
    // In production environment, should use Avro schema for strict definition and parsing
    Map<String, Integer> countByValueFields = new HashMap<>();
    try {
      int fieldCount = decoder.readInt();
      for (int i = 0; i < fieldCount; i++) {
        String fieldName = decoder.readString();
        int topK = decoder.readInt();
        countByValueFields.put(fieldName, topK);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<ComputeRouterRequestKeyV1> keys = DESERIALIZER.deserializeObjects(decoder);
    String schemaId = httpRequest.headers().get(HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID);
    return new AggregationRouterRequestWrapper(resourceName, countByValueFields, keys, httpRequest, schemaId);
  }

  public Map<String, Integer> getCountByValueFields() {
    return countByValueFields;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.AGGREGATION;
  }

  @Override
  public String toString() {
    return "AggregationRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + getKeyCount() + ")";
  }
}
