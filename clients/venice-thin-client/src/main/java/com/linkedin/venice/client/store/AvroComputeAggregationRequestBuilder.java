package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.schema.SchemaReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


/**
 * Implementation of {@link ComputeAggregationRequestBuilder} that supports counting field values
 * and grouping them by their values and buckets. This implementation works completely on the client side
 * without requiring any server-side compute functionality.
 */
public class AvroComputeAggregationRequestBuilder<K> implements ComputeAggregationRequestBuilder<K> {
  private final AvroGenericStoreClient<K, Object> storeClient;
  private final Map<String, Integer> fieldTopKMap = new HashMap<>();
  private final Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
  private final SchemaReader schemaReader;

  public AvroComputeAggregationRequestBuilder(
      AvroGenericStoreClient<K, Object> storeClient,
      SchemaReader schemaReader) {
    this.storeClient = storeClient;
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

    // Store topK value for each field
    for (String fieldName: fieldNames) {
      fieldTopKMap.put(fieldName, topK);
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

    // Store bucket predicates for each field
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
    }
    return this;
  }

  @Override
  public CompletableFuture<ComputeAggregationResponse> execute(Set<K> keys) throws VeniceClientException {
    System.out.println("=== CLIENT SIDE AGG EXECUTE ===");
    if (keys == null || keys.isEmpty()) {
      throw new VeniceClientException("keys cannot be null or empty");
    }

    // Execute pure client-side aggregation
    return CompletableFuture.supplyAsync(() -> {
      try {
        // Fetch all data from the store
        Map<K, Object> allData = new HashMap<>();
        for (K key: keys) {
          try {
            Object value = storeClient.get(key).get(15, TimeUnit.SECONDS);
            if (value != null) {
              allData.put(key, value);
            }
          } catch (Exception e) {
            // Skip keys that fail to fetch
            System.err.println("Failed to fetch key " + key + ": " + e.getMessage());
          }
        }

        // Convert to ComputeGenericRecord format for compatibility
        Map<K, ComputeGenericRecord> computeResults = new HashMap<>();
        for (Map.Entry<K, Object> entry: allData.entrySet()) {
          if (entry.getValue() instanceof GenericRecord) {
            GenericRecord record = (GenericRecord) entry.getValue();
            computeResults.put(entry.getKey(), new ClientSideComputeGenericRecord(record));
          }
        }

        // Create and return the aggregation response
        return new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap, fieldBucketMap);
      } catch (Exception e) {
        throw new VeniceClientException("Failed to execute client-side aggregation", e);
      }
    });
  }

  /**
   * Simple wrapper class to make GenericRecord compatible with ComputeGenericRecord
   */
  private static class ClientSideComputeGenericRecord extends ComputeGenericRecord {
    private final GenericRecord record;

    public ClientSideComputeGenericRecord(GenericRecord record) {
      super(createMockRecordWithErrorField(), record.getSchema());
      this.record = record;
    }

    private static GenericRecord createMockRecordWithErrorField() {
      // Create a simple mock record with just the error field
      Schema.Field errorField = new Schema.Field(
          "__veniceComputationError__",
          Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createMap(Schema.create(Schema.Type.STRING))),
          null,
          null);

      Schema mockSchema = Schema.createRecord("MockRecord", null, null, false, Arrays.asList(errorField));

      GenericRecord mockRecord = new GenericData.Record(mockSchema);
      mockRecord.put("__veniceComputationError__", null);

      return mockRecord;
    }

    @Override
    public void put(String key, Object v) {
      record.put(key, v);
    }

    @Override
    public Object get(String key) {
      return record.get(key);
    }

    @Override
    public void put(int i, Object v) {
      record.put(i, v);
    }

    @Override
    public Object get(int i) {
      return record.get(i);
    }

    @Override
    public Schema getSchema() {
      return record.getSchema();
    }
  }
}
