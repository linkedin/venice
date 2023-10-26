package com.linkedin.venice.client.store;

import static org.apache.avro.Schema.Type.RECORD;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaAndToString;
import com.linkedin.venice.client.store.predicate.AndPredicate;
import com.linkedin.venice.client.store.predicate.EqualsRelationalOperator;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class AvroComputeRequestBuilderV4<K> extends AvroComputeRequestBuilderV3<K> {
  public AvroComputeRequestBuilderV4(AvroGenericReadComputeStoreClient storeClient, SchemaReader schemaReader) {
    super(storeClient, schemaReader);
  }

  @Override
  public void executeWithFilter(
      Predicate requiredPrefixFields,
      StreamingCallback<GenericRecord, GenericRecord> callback) {
    byte[] prefixBytes = extractKeyPrefixBytesFromPredicate(requiredPrefixFields, storeClient.getKeySchema());
    SchemaAndToString resultSchema = getResultSchema();
    ComputeRequestWrapper computeRequestWrapper = generateComputeRequest(resultSchema, true);
    storeClient.computeWithKeyPrefixFilter(prefixBytes, computeRequestWrapper, callback);
  }

  private byte[] extractKeyPrefixBytesFromPredicate(Predicate requiredPrefixFields, Schema keySchema) {
    if (requiredPrefixFields == null) {
      return null;
    }

    if (keySchema == null) {
      throw new VeniceClientException("Key schema cannot be null");
    } else if (RECORD != keySchema.getType()) {
      throw new VeniceClientException("Key schema must be of type Record to execute with a filter on key fields");
    }

    Map<String, Object> keyFieldsFromPredicate = new HashMap<>();
    populateKeyFieldMapFromPredicate(requiredPrefixFields, keyFieldsFromPredicate);

    List<Schema.Field> prefixFields =
        getAndCheckExpectedPrefixFields(keySchema.getFields(), keyFieldsFromPredicate.keySet());
    Schema prefixSchema = Schema.createRecord("prefixSchema", "", "", false);
    prefixSchema.setFields(prefixFields);

    GenericData.Record prefix = new GenericData.Record(prefixSchema);

    for (Map.Entry<String, Object> keyField: keyFieldsFromPredicate.entrySet()) {
      prefix.put(keyField.getKey(), keyField.getValue());
    }

    try {
      RecordSerializer<GenericRecord> serializer =
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(prefixSchema, false);
      return serializer.serialize(prefix);
    } catch (Exception e) {
      throw new VeniceClientException(
          "Cannot serialize partial key. Please ensure the leading fields are completely specified",
          e);
    }
  }

  private void populateKeyFieldMapFromPredicate(Predicate predicate, Map<String, Object> keyFields) {
    if (predicate instanceof AndPredicate) {
      List<Predicate> childPredicates = ((AndPredicate) predicate).getChildPredicates();
      for (Predicate p: childPredicates) {
        populateKeyFieldMapFromPredicate(p, keyFields);
      }
    } else if (predicate instanceof EqualsRelationalOperator) {
      EqualsRelationalOperator equalsPredicate = (EqualsRelationalOperator) predicate;
      if (keyFields.containsKey(equalsPredicate.getFieldName())
          && !keyFields.get(equalsPredicate.getFieldName()).equals(equalsPredicate.getExpectedValue())) {
        throw new VeniceException("Key field \"" + equalsPredicate.getFieldName() + "\" cannot have multiple values");
      }
      keyFields.put(equalsPredicate.getFieldName(), equalsPredicate.getExpectedValue());
    } else {
      throw new VeniceException(
          "Invalid filtering predicate. Filtering predicate can only contain AND and EQUALS operators");
    }
  }

  private List<Schema.Field> getAndCheckExpectedPrefixFields(
      List<Schema.Field> keySchemaFields,
      Set<String> expectedPrefixKeys) {
    if (expectedPrefixKeys.isEmpty()) {
      throw new VeniceException("Predicate must contain at least one key field");
    }

    List<Schema.Field> prefixFields = new ArrayList<>();

    for (Schema.Field keyField: keySchemaFields) {
      if (expectedPrefixKeys.contains(keyField.name())) {
        prefixFields.add(AvroCompatibilityHelper.createSchemaField(keyField.name(), keyField.schema(), "", null));
      } else {
        break;
      }
    }

    if (prefixFields.size() != expectedPrefixKeys.size()) {
      throw new VeniceException("The specified key fields must be leading fields in the key schema");
    }

    return prefixFields;
  }
}
