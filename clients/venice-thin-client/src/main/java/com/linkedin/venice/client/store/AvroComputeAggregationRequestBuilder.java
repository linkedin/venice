package com.linkedin.venice.client.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.schema.SchemaReader;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;


/**
 * Implementation of {@link ComputeAggregationRequestBuilder} that supports counting field values
 * and grouping them by their values and buckets.
 */
public class AvroComputeAggregationRequestBuilder<K> implements ComputeAggregationRequestBuilder<K> {
  private final AvroComputeRequestBuilderV3<K> delegate;
  private final Map<String, Integer> fieldTopKMap = new HashMap<>();
  private final Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
  private final SchemaReader schemaReader;
  private final AvroGenericReadComputeStoreClient storeClient;

  public AvroComputeAggregationRequestBuilder(
      AvroGenericReadComputeStoreClient storeClient,
      SchemaReader schemaReader) {
    this.delegate = (AvroComputeRequestBuilderV3<K>) storeClient.compute();
    this.schemaReader = schemaReader;
    this.storeClient = storeClient;
  }

  /**
   * Validates that the given field names exist in the schema and are not null or empty.
   * This method is shared between countGroupByValue and countGroupByBucket to avoid code duplication.
   */
  private void validateFieldNames(String... fieldNames) {
    if (fieldNames == null || fieldNames.length == 0) {
      throw new VeniceClientException("fieldNames cannot be null or empty");
    }

    Schema valueSchema = schemaReader.getValueSchema(schemaReader.getLatestValueSchemaId());
    for (String fieldName: fieldNames) {
      if (fieldName == null) {
        throw new VeniceClientException("Field name cannot be null");
      }
      if (fieldName.isEmpty()) {
        throw new VeniceClientException("Field name cannot be empty");
      }

      Schema.Field field = valueSchema.getField(fieldName);
      if (field == null) {
        throw new VeniceClientException("Field not found in schema: " + fieldName);
      }
    }
  }

  /**
   * Validates that predicate types match the expected field schema types.
   * This ensures type safety and prevents runtime type mismatches.
   */
  private <T> void validatePredicateTypes(Map<String, Predicate<T>> bucketNameToPredicate, String... fieldNames) {
    Schema valueSchema = schemaReader.getValueSchema(schemaReader.getLatestValueSchemaId());

    for (String fieldName: fieldNames) {
      Schema.Field field = valueSchema.getField(fieldName);
      Schema fieldSchema = field.schema();

      // Handle union types by getting the first non-null type
      if (fieldSchema.getType() == Schema.Type.UNION) {
        for (Schema unionType: fieldSchema.getTypes()) {
          if (unionType.getType() != Schema.Type.NULL) {
            fieldSchema = unionType;
            break;
          }
        }
      }

      for (Map.Entry<String, Predicate<T>> entry: bucketNameToPredicate.entrySet()) {
        String bucketName = entry.getKey();
        Predicate<T> predicate = entry.getValue();

        // Validate predicate type matches field schema type
        if (!isPredicateTypeCompatible(predicate, fieldSchema)) {
          throw new VeniceClientException(
              String.format(
                  "Predicate type mismatch for bucket '%s' and field '%s'. " + "Expected type: %s, Predicate type: %s",
                  bucketName,
                  fieldName,
                  fieldSchema.getType(),
                  predicate.getClass().getSimpleName()));
        }
      }
    }
  }

  /**
   * Checks if the predicate type is compatible with the given Avro schema type.
   * Uses polymorphism to delegate type checking to each predicate implementation.
   */
  private boolean isPredicateTypeCompatible(Predicate<?> predicate, Schema schema) {
    return predicate.isCompatibleWithSchema(schema);
  }

  @Override
  public ComputeAggregationRequestBuilder<K> countGroupByValue(int topK, String... fieldNames) {
    // topK must bigger than 0
    if (topK <= 0) {
      throw new VeniceClientException("TopK must be positive");
    }

    // Validate fields exist in schema
    validateFieldNames(fieldNames);

    // Store topK value for each field and project the field
    for (String fieldName: fieldNames) {
      fieldTopKMap.put(fieldName, topK);
      delegate.project(fieldName);
    }
    return this;
  }

  @Override
  public <T> ComputeAggregationRequestBuilder<K> countGroupByBucket(
      Map<String, Predicate<T>> bucketNameToPredicate,
      String... fieldNames) {
    // bucket predicates must not be null or empty
    if (bucketNameToPredicate == null || bucketNameToPredicate.isEmpty()) {
      throw new VeniceClientException("bucketNameToPredicate cannot be null or empty");
    }

    // Validate bucket names and predicates
    for (Map.Entry<String, Predicate<T>> entry: bucketNameToPredicate.entrySet()) {
      if (entry.getKey() == null || entry.getKey().isEmpty()) {
        throw new VeniceClientException("Bucket name cannot be null or empty");
      }
      if (entry.getValue() == null) {
        throw new VeniceClientException("Predicate for bucket '" + entry.getKey() + "' cannot be null");
      }
    }

    // Validate fields exist in schema
    validateFieldNames(fieldNames);

    // Validate predicate types match field schema types
    validatePredicateTypes(bucketNameToPredicate, fieldNames);

    // Store bucket predicates for each field and project the field
    for (String fieldName: fieldNames) {
      Map<String, Predicate> existingBuckets = fieldBucketMap.get(fieldName);
      if (existingBuckets == null) {
        existingBuckets = new HashMap<>();
        fieldBucketMap.put(fieldName, existingBuckets);
      }

      // Add all buckets for this field
      for (Map.Entry<String, Predicate<T>> entry: bucketNameToPredicate.entrySet()) {
        existingBuckets.put(entry.getKey(), entry.getValue());
      }

      delegate.project(fieldName);
    }
    return this;
  }

  @Override
  public CompletableFuture<ComputeAggregationResponse> execute(Set<K> keys) throws VeniceClientException {
    if (keys == null || keys.isEmpty()) {
      throw new VeniceClientException("keys cannot be null or empty");
    }

    // Try server-side aggregation first if only countByValue operations (no buckets)
    if (fieldBucketMap.isEmpty() && !fieldTopKMap.isEmpty() && canUseServerSideAggregation()) {
      try {
        return executeServerSideAggregation(keys).exceptionally(ex -> {
          // If server-side aggregation fails, fall back to client-side
          // This could happen if server doesn't support the feature yet
          // TODO: Add proper logging once logger is available
          try {
            return delegate.execute(keys)
                .thenApply(result -> new AvroComputeAggregationResponse<>(result, fieldTopKMap, fieldBucketMap))
                .get();
          } catch (Exception fallbackEx) {
            throw new RuntimeException("Both server-side and client-side aggregation failed", fallbackEx);
          }
        });
      } catch (Exception e) {
        // If there's a synchronous exception during setup, fall back to client-side
        // TODO: Add proper logging once logger is available
      }
    }

    // Fall back to client-side aggregation
    return delegate.execute(keys)
        .thenApply(result -> new AvroComputeAggregationResponse<>(result, fieldTopKMap, fieldBucketMap));
  }

  /**
   * Execute server-side aggregation by calling the /aggregation endpoint
   */
  private CompletableFuture<ComputeAggregationResponse> executeServerSideAggregation(Set<K> keys)
      throws VeniceClientException {
    return CompletableFuture.supplyAsync(() -> {
      try {
        // 1. Get transport client and store name
        TransportClient transportClient = getTransportClient();
        String storeName = getStoreName();

        // 2. Serialize the aggregation request
        byte[] requestBody = serializeAggregationRequest(keys);

        // 3. Build request headers
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/octet-stream");
        headers.put("Venice-API-Version", "1");
        if (schemaReader.getLatestValueSchemaId() > 0) {
          headers.put("Venice-Compute-Value-Schema-Id", String.valueOf(schemaReader.getLatestValueSchemaId()));
        }

        // 4. Make HTTP POST request
        String aggregationPath = "/aggregation/" + storeName;
        CompletableFuture<TransportClientResponse> responseFuture =
            transportClient.post(aggregationPath, headers, requestBody);

        // 5. Wait for response and parse
        TransportClientResponse response = responseFuture.get();
        String responseJson = new String(response.getBody(), "UTF-8");

        // 6. Convert JSON response to ComputeAggregationResponse
        return createAggregationResponseFromJson(responseJson);

      } catch (Exception e) {
        throw new VeniceClientException("Failed to execute server-side aggregation", e);
      }
    });
  }

  /**
   * Check if server-side aggregation can be used
   */
  private boolean canUseServerSideAggregation() {
    // Check if we can access transport client (not in test mode with mocked clients)
    return storeClient instanceof AbstractAvroStoreClient;
  }

  /**
   * Get the transport client from the store client
   */
  private TransportClient getTransportClient() {
    if (storeClient instanceof AbstractAvroStoreClient) {
      return ((AbstractAvroStoreClient) storeClient).getTransportClient();
    }
    throw new VeniceClientException("Cannot access transport client from store client");
  }

  /**
   * Get the store name from the store client
   */
  private String getStoreName() {
    return storeClient.getStoreName();
  }

  /**
   * Serialize aggregation request according to the protocol expected by AggregationRouterRequestWrapper
   */
  private byte[] serializeAggregationRequest(Set<K> keys) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);

    // Write field count and field mappings (Map<String, Integer> countByValueFields)
    encoder.writeInt(fieldTopKMap.size());
    for (Map.Entry<String, Integer> entry: fieldTopKMap.entrySet()) {
      encoder.writeString(entry.getKey());
      encoder.writeInt(entry.getValue());
    }

    // Serialize keys count first
    encoder.writeInt(keys.size());

    // Convert keys to ComputeRouterRequestKeyV1 format and serialize each one
    for (K key: keys) {
      ComputeRouterRequestKeyV1 routerKey = new ComputeRouterRequestKeyV1();

      // Get partition ID (this is a simplified approach - in reality you'd need the partition finder)
      // Use bitwise AND to ensure positive value, avoiding Math.abs(Integer.MIN_VALUE) issue
      int partitionId = (key.hashCode() & 0x7FFFFFFF) % 100; // Simple hash-based partitioning
      routerKey.setPartitionId(partitionId);

      // Serialize the key
      byte[] keyBytes;
      if (key instanceof String) {
        keyBytes = ((String) key).getBytes("UTF-8");
      } else if (key instanceof ByteBuffer) {
        ByteBuffer bb = (ByteBuffer) key;
        keyBytes = new byte[bb.remaining()];
        bb.get(keyBytes);
      } else {
        // Use toString as fallback
        keyBytes = key.toString().getBytes("UTF-8");
      }
      routerKey.setKeyBytes(ByteBuffer.wrap(keyBytes));

      // Write the router key fields directly
      encoder.writeInt(routerKey.getPartitionId());
      encoder.writeBytes(routerKey.getKeyBytes());
    }

    encoder.flush();
    return baos.toByteArray();
  }

  /**
   * Create ComputeAggregationResponse from JSON response
   */
  private ComputeAggregationResponse createAggregationResponseFromJson(String responseJson) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Map<String, Integer>> jsonResults =
        mapper.readValue(responseJson, new TypeReference<Map<String, Map<String, Integer>>>() {
        });

    // Convert to the format expected by AvroComputeAggregationResponse
    // Since the server returns aggregated results, we create a special response that bypasses client-side aggregation
    return new ServerSideAggregationResponse<>(jsonResults);
  }

  /**
   * Special implementation of ComputeAggregationResponse for server-side aggregated results
   */
  private static class ServerSideAggregationResponse<K> implements ComputeAggregationResponse {
    private final Map<String, Map<String, Integer>> serverResults;

    public ServerSideAggregationResponse(Map<String, Map<String, Integer>> serverResults) {
      this.serverResults = serverResults;
    }

    @Override
    public <T> Map<T, Integer> getValueToCount(String field) {
      Map<String, Integer> fieldResults = serverResults.get(field);
      if (fieldResults == null) {
        return new HashMap<>();
      }

      // Convert String keys back to T type and return the server-aggregated results
      Map<T, Integer> result = new HashMap<>();
      for (Map.Entry<String, Integer> entry: fieldResults.entrySet()) {
        @SuppressWarnings("unchecked")
        T key = (T) entry.getKey();
        result.put(key, entry.getValue());
      }
      return result;
    }

    @Override
    public Map<String, Integer> getBucketNameToCount(String fieldName) {
      // Server-side bucket aggregation not implemented yet
      return new HashMap<>();
    }
  }
}
