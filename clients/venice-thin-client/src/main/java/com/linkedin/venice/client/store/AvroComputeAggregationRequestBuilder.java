package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.schema.SchemaReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;


/**
 * Implementation of {@link ComputeAggregationRequestBuilder} that supports counting field values
 * and grouping them by their values and buckets.
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
                  getPredicateType(predicate)));
        }
      }
    }
  }

  /**
   * Checks if the predicate type is compatible with the given Avro schema type.
   */
  private boolean isPredicateTypeCompatible(Predicate<?> predicate, Schema schema) {
    Schema.Type avroType = schema.getType();

    if (predicate instanceof LongPredicate) {
      return avroType == Schema.Type.LONG || avroType == Schema.Type.INT;
    } else if (predicate instanceof IntPredicate) {
      return avroType == Schema.Type.INT;
    } else if (predicate instanceof FloatPredicate) {
      return avroType == Schema.Type.FLOAT || avroType == Schema.Type.INT;
    } else if (predicate instanceof DoublePredicate) {
      return avroType == Schema.Type.DOUBLE || avroType == Schema.Type.FLOAT || avroType == Schema.Type.INT;
    } else {
      // For generic predicates, we allow them to work with any type
      // The actual type checking will happen at runtime
      return true;
    }
  }

  /**
   * Gets a human-readable description of the predicate type.
   */
  private String getPredicateType(Predicate<?> predicate) {
    if (predicate instanceof LongPredicate) {
      return "LongPredicate";
    } else if (predicate instanceof IntPredicate) {
      return "IntPredicate";
    } else if (predicate instanceof FloatPredicate) {
      return "FloatPredicate";
    } else if (predicate instanceof DoublePredicate) {
      return "DoublePredicate";
    } else {
      return "GenericPredicate";
    }
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

    // Execute the compute request
    return delegate.execute(keys)
        .thenApply(result -> new AvroComputeAggregationResponse<>(result, fieldTopKMap, fieldBucketMap));
  }
}
