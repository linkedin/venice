package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.schema.SchemaReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;


/**
 * Implementation of {@link ComputeAggregationRequestBuilder} that supports counting field values
 * and grouping them by their values.
 */
public class AvroComputeAggregationRequestBuilder<K> implements ComputeAggregationRequestBuilder<K> {
  private final AvroComputeRequestBuilderV3<K> delegate;
  private final Map<String, Integer> fieldTopKMap = new HashMap<>();
  private final Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
  private final SchemaReader schemaReader;

  public AvroComputeAggregationRequestBuilder(
      AvroGenericReadComputeStoreClient storeClient,
      SchemaReader schemaReader) {
    this.delegate = (AvroComputeRequestBuilderV3<K>) storeClient.compute();
    this.schemaReader = schemaReader;
  }

  @Override
  public ComputeAggregationRequestBuilder<K> countGroupByValue(int topK, String... fieldNames) {
    // topK must bigger than 0
    if (topK <= 0) {
      throw new VeniceClientException("TopK must be positive");
    }
    // field name must not be empty
    if (fieldNames == null || fieldNames.length == 0) {
      throw new VeniceClientException("fieldNames cannot be null or empty");
    }

    // Validate fields exist in schema
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

      // Store topK value for each field
      fieldTopKMap.put(fieldName, topK);

      // For countGroupByValue, we need to project the field itself
      delegate.project(fieldName);
    }
    return this;
  }

  @Override
  public <T> ComputeAggregationRequestBuilder<K> countGroupByBucket(
      Map<String, Predicate<T>> bucketNameToPredicate,
      String... fieldNames) {
    // Validate inputs
    if (bucketNameToPredicate == null || bucketNameToPredicate.isEmpty()) {
      throw new VeniceClientException("bucketNameToPredicate cannot be null or empty");
    }
    if (fieldNames == null || fieldNames.length == 0) {
      throw new VeniceClientException("fieldNames cannot be null or empty");
    }

    // Validate bucket names
    for (Map.Entry<String, Predicate<T>> entry: bucketNameToPredicate.entrySet()) {
      if (entry.getKey() == null || entry.getKey().isEmpty()) {
        throw new VeniceClientException("Bucket name cannot be null or empty");
      }
      if (entry.getValue() == null) {
        throw new VeniceClientException("Predicate for bucket '" + entry.getKey() + "' cannot be null");
      }
    }

    // Validate fields exist in schema
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

      // Store bucket predicates for each field
      Map<String, Predicate> existingBuckets = fieldBucketMap.get(fieldName);
      if (existingBuckets == null) {
        existingBuckets = new HashMap<>();
        fieldBucketMap.put(fieldName, existingBuckets);
      }

      // Add all buckets for this field
      for (Map.Entry<String, Predicate<T>> entry: bucketNameToPredicate.entrySet()) {
        existingBuckets.put(entry.getKey(), entry.getValue());
      }

      // Project the field so we can process the values in the response
      delegate.project(fieldName);
    }
    return this;
  }

  @Override
  public CompletableFuture<ComputeAggregationResponse> execute(Set<K> keys) throws VeniceClientException {
    if (keys == null || keys.isEmpty()) {
      throw new VeniceClientException("keys cannot be null or empty");
    }

    // Execute the compute request
    return delegate.execute(keys)
        .thenApply(result -> new AvroComputeAggregationResponse<>(result, fieldTopKMap, fieldBucketMap));
  }
}
