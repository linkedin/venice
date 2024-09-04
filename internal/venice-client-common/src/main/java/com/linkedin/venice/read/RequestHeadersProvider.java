package com.linkedin.venice.read;

import static com.linkedin.venice.HttpConstants.VENICE_CLIENT_COMPUTE;
import static com.linkedin.venice.HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID;
import static com.linkedin.venice.HttpConstants.VENICE_KEY_COUNT;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Provide the appropriate headers based on the request type for all remote Venice clients. i.e. the thin-client and
 * the fast-client.
 */
public class RequestHeadersProvider {
  private static final Logger LOGGER = LogManager.getLogger(RequestHeadersProvider.class);
  private static final Map<String, String> GET_HEADER_MAP = new HashMap<>();
  private static final Map<String, String> STREAMING_MULTI_GET_HEADER_MAP = new HashMap<>();
  private static final Map<String, String> STREAMING_COMPUTE_HEADER_MAP_V3 = new HashMap<>();

  static {
    /**
     * Hard-code API version of single-get and multi-get to be '1'.
     * If the header varies request by request, Venice client needs to create a map per request.
     */
    GET_HEADER_MAP.put(
        HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_CLIENT_REQUEST_V1.getProtocolVersion()));
    GET_HEADER_MAP.put(
        HttpConstants.VENICE_SUPPORTED_COMPRESSION_STRATEGY,
        Integer.toString(CompressionStrategy.GZIP.getValue()));

    STREAMING_MULTI_GET_HEADER_MAP.put(
        HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getProtocolVersion()));

    /**
     * COMPUTE_REQUEST_V1 and V2 are deprecated.
     */
    STREAMING_COMPUTE_HEADER_MAP_V3.put(
        HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.COMPUTE_REQUEST_V3.getProtocolVersion()));
    STREAMING_COMPUTE_HEADER_MAP_V3.put(HttpConstants.VENICE_STREAMING, "1");

    AvroVersion version = AvroCompatibilityHelperCommon.getRuntimeAvroVersion();
    LOGGER.info("Detected: {} on the classpath.", version);
  }

  public static Map<String, String> getThinClientGetHeaderMap() {
    return new HashMap<>(GET_HEADER_MAP);
  }

  public static Map<String, String> getThinClientStreamingBatchGetHeaders(int keyCount) {
    Map<String, String> headers = getStreamingBatchGetHeaders(keyCount);
    headers.put(HttpConstants.VENICE_STREAMING, "1");
    headers.put(
        HttpConstants.VENICE_SUPPORTED_COMPRESSION_STRATEGY,
        Integer.toString(CompressionStrategy.GZIP.getValue()));
    return headers;
  }

  public static Map<String, String> getStreamingBatchGetHeaders(int keyCount) {
    Map<String, String> headers = new HashMap<>(STREAMING_MULTI_GET_HEADER_MAP.size() + 1);
    headers.putAll(STREAMING_MULTI_GET_HEADER_MAP);
    headers.put(VENICE_KEY_COUNT, Integer.toString(keyCount));
    return headers;
  }

  public static Map<String, String> getStreamingComputeHeaderMap(
      int keyCount,
      int computeValueSchemaId,
      boolean isRemoteComputationOnly) {
    Map<String, String> headers = new HashMap<>(STREAMING_COMPUTE_HEADER_MAP_V3.size() + 3);
    headers.putAll(STREAMING_COMPUTE_HEADER_MAP_V3);
    headers.put(VENICE_KEY_COUNT, Integer.toString(keyCount));
    headers.put(VENICE_COMPUTE_VALUE_SCHEMA_ID, Integer.toString(computeValueSchemaId));
    if (!isRemoteComputationOnly) {
      headers.put(VENICE_CLIENT_COMPUTE, "1");
    }
    return headers;
  }
}
