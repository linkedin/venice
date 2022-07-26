package com.linkedin.venice.writer.update;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.writecompute.WriteComputeConstants;
import com.linkedin.venice.serializer.AvroSerializer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Validate;


public class UpdateBuilderImpl implements UpdateBuilder {

  private final GenericRecord updateRecord;
  private final Map<String, List<?>> toAddElementsByFieldName;
  private final Map<String, List<?>> toRemoveElementsByFieldName;
  private final Map<String, List<String>> toRemoveKeysByFieldName;
  private final Map<String, Map<String, ?>> toAddEntriesByFieldName;
  private final AvroSerializer<GenericRecord> serializer;
  private boolean anyUpdate;

  /**
   * @param updateSchema Update schema that is derived from the value Record schema.
   */
  public UpdateBuilderImpl(Schema updateSchema) {
    validateUpdateSchema(updateSchema);
    this.updateRecord = new GenericData.Record(updateSchema);
    this.serializer = new AvroSerializer<>(updateSchema);
    this.toAddElementsByFieldName = new HashMap<>();
    this.toRemoveElementsByFieldName = new HashMap<>();
    this.toRemoveKeysByFieldName = new HashMap<>();
    this.toAddEntriesByFieldName = new HashMap<>();
    this.anyUpdate = false;
  }

  private void validateUpdateSchema(Schema updateSchema) {
    if (updateSchema.getType() != Schema.Type.RECORD ||
        !updateSchema.getName().endsWith(WriteComputeConstants.WRITE_COMPUTE_RECORD_SCHEMA_SUFFIX)) {
      throw new IllegalArgumentException("Got invalid record-update schema: " + updateSchema);
    }
  }

  @Override
  public UpdateBuilder setNewFieldValue(String fieldName, Object newFieldValue) {
    validateNoCollectionMergeOnField(fieldName);
    updateRecord.put(fieldName, newFieldValue);
    anyUpdate = true;
    return this;
  }

  @Override
  public UpdateBuilder setElementsToAddToListField(String listFieldName, List<?> elementsToAdd) {
    validateFieldType(Validate.notNull(listFieldName), Schema.Type.ARRAY);
    validateFieldNotSet(listFieldName);
    if (!elementsToAdd.isEmpty()) {
      toAddElementsByFieldName.put(listFieldName, elementsToAdd);
      anyUpdate = true;
    }
    return this;
  }

  @Override
  public UpdateBuilder setElementsToRemoveFromListField(String listFieldName, List<?> elementsToRemove) {
    validateFieldType(Validate.notNull(listFieldName), Schema.Type.ARRAY);
    validateFieldNotSet(listFieldName);
    if (!elementsToRemove.isEmpty()) {
      toRemoveElementsByFieldName.put(listFieldName, elementsToRemove);
      anyUpdate = true;
    }
    return this;
  }

  @Override
  public UpdateBuilder setEntriesToAddToMapField(String mapFieldName, Map<String, ?> entriesToAdd) {
    validateFieldType(Validate.notNull(mapFieldName), Schema.Type.MAP);
    validateFieldNotSet(mapFieldName);
    if (!entriesToAdd.isEmpty()) {
      toAddEntriesByFieldName.put(mapFieldName, entriesToAdd);
      anyUpdate = true;
    }
    return this;
  }

  @Override
  public UpdateBuilder setKeysToRemoveFromMapField(String mapFieldName, List<String> keysToRemove) {
    validateFieldType(Validate.notNull(mapFieldName), Schema.Type.MAP);
    validateFieldNotSet(mapFieldName);
    if (!keysToRemove.isEmpty()) {
      toRemoveKeysByFieldName.put(mapFieldName, keysToRemove);
      anyUpdate = true;
    }
    return this;
  }

  @Override
  public GenericRecord build() {
    if (!anyUpdate) {
      throw new IllegalStateException("No update has been specified. Please use setter methods to specify how to partially "
          + "update a value record before calling this build method.");
    }
    for (Schema.Field updateField : updateRecord.getSchema().getFields()) {
      final String fieldName = updateField.name();
      if (updateRecord.get(fieldName) != null) {
        continue; // This field already has a new value.
      }
      updateRecord.put(fieldName, createUpdateFieldValue(updateField));
    }
    Exception serializationException = validateUpdateRecordIsSerializable(updateRecord).orElse(null);
    if (serializationException != null) {
      throw new VeniceException("The built partial-update record failed to be serialized. It could be caused by setting "
          + "field value(s) with wrong type(s). Built record: " + updateRecord + ", and serialization exception: ", serializationException);
    }
    return updateRecord;
  }

  private Optional<Exception> validateUpdateRecordIsSerializable(GenericRecord updateRecord) {
    try {
      serializer.serialize(updateRecord);
    } catch (Exception serializationException) {
      return Optional.of(serializationException);
    }
    return Optional.empty();
  }

  /**
   * Create a {@link GenericRecord} for a given field. The created {@link GenericRecord} represents how to update a field
   * in a value record, specifically there could be 3 types of field update:
   *    1. No-op
   *    2. Add/remove elements to/from a List field.
   *    3. Add/remove K-V pairs to/from a Map field.
   *
   * @param updateField A field from the Write Compute schema. Its field name is the same as the field name in the value
   *                    schema. It also contains the schema that can be used to specify how to update a value field.
   */
  private GenericRecord createUpdateFieldValue(Schema.Field updateField) {
    Schema.Type valueFieldType = getCorrespondingValueFieldType(updateField);
    switch (valueFieldType) {
      case ARRAY:
        List<?> toAddElements = toAddElementsByFieldName.getOrDefault(updateField.name(), Collections.emptyList());
        List<?> toRemoveElements = toRemoveElementsByFieldName.getOrDefault(updateField.name(), Collections.emptyList());
        if (toAddElements.isEmpty() && toRemoveElements.isEmpty()) {
          return createNoOpRecord(updateField); // No update on this List field.
        }
        return createListMergeRecord(updateField, toAddElements, toRemoveElements);

      case MAP:
        List<String> toRemoveKeys = toRemoveKeysByFieldName.getOrDefault(updateField.name(), Collections.emptyList());
        Map<String, ?> toAddEntries = toAddEntriesByFieldName.getOrDefault(updateField.name(), Collections.emptyMap());
        removeSameKeys(toAddEntries, toRemoveKeys);
        if (toRemoveKeys.isEmpty() && toAddEntries.isEmpty()) {
          return createNoOpRecord(updateField);  // No update on this Map field.
        }
        return createMapMergeRecord(updateField, toAddEntries, toRemoveKeys);

      default:
        return createNoOpRecord(updateField);
    }
  }

  /**
   * If a key is in the to-remove-key list and also in the to-add-entry list, this key is removed from both lists.
   */
  private void removeSameKeys(Map<String, ?> toAddEntries, List<String> toRemoveKeys) {
    List<String> remainingToRemoveKeys = new LinkedList<>();
    for (String toRemoveKey : toRemoveKeys) {
      if (toAddEntries.remove(toRemoveKey) == null) {
        remainingToRemoveKeys.add(toRemoveKey);
      }
    }
    if (remainingToRemoveKeys.size() < toRemoveKeys.size()) {
      toRemoveKeys.clear();
      toRemoveKeys.addAll( remainingToRemoveKeys);
    }
  }

  /**
   * Given a field from the Write Compute schema and find the type of its corresponding value field.
   *
   * @param updateField A field from the Write Compute schema.
   * @return Type of its corresponding value field.
   */
  private Schema.Type getCorrespondingValueFieldType(Schema.Field updateField) {
    // Each field in Write Compute Update schema is a union of multiple schemas.
    List<Schema> updateFieldSchemas = updateField.schema().getTypes();
    // The last schema in the union is the schema of the field in the corresponding value schema.
    return updateFieldSchemas.get(updateFieldSchemas.size() - 1).getType();
  }

  private void validateFieldNotSet(String fieldName) {
    Object existingFieldValue = updateRecord.get(fieldName);
    if (existingFieldValue != null) {
      throw new IllegalStateException("Field " + fieldName + " has already been set with value: " + existingFieldValue);
    }
  }

  private void validateFieldType(String fieldName, Schema.Type expectedType) {
    Schema.Field updateField = updateRecord.getSchema().getField(fieldName);
    if (updateField == null) {
      throw new IllegalStateException();
    }
    Schema.Type valueFieldType = getCorrespondingValueFieldType(updateField);
    if (valueFieldType != expectedType) {
      throw new IllegalStateException();
    }
  }

  private void validateNoCollectionMergeOnField(String fieldName) {
    if (toAddElementsByFieldName.containsKey(fieldName)) {
      throw new IllegalStateException("Field " + fieldName + " already had to-add element(s).");
    }
    if (toRemoveElementsByFieldName.containsKey(fieldName)) {
      throw new IllegalStateException("Field " + fieldName + " already had to-remove element(s).");
    }
    if (toRemoveKeysByFieldName.containsKey(fieldName)) {
      throw new IllegalStateException("Field " + fieldName + " already had to-remove key(s).");
    }
    if (toAddEntriesByFieldName.containsKey(fieldName)) {
      throw new IllegalStateException("Field " + fieldName + " already had to-add entries.");
    }
  }

  private GenericRecord createNoOpRecord(Schema.Field updateField) {
    // Because of the way how Write Compute schema is generated, the first entry in the every union write compute field
    // must be the no-op schema.
    Schema noOpRecordSchema = updateField.schema().getTypes().get(0);
    return new GenericData.Record(noOpRecordSchema);
  }

  private GenericRecord createListMergeRecord(Schema.Field updateField, List<?> toAdd, List<?> toRemove) {
    Schema listMergeRecordSchema = updateField.schema().getTypes().get(1);
    GenericRecord listMergeRecord = new GenericData.Record(listMergeRecordSchema);
    listMergeRecord.put(WriteComputeConstants.SET_UNION, toAdd);
    listMergeRecord.put(WriteComputeConstants.SET_DIFF, toRemove);
    return listMergeRecord;
  }

  private GenericRecord createMapMergeRecord(Schema.Field updateField, Map<String, ?> toAdd, List<String> toRemoveKeys) {
    Schema mapMergeRecordSchema = updateField.schema().getTypes().get(1);
    GenericRecord mapMergeRecord = new GenericData.Record(mapMergeRecordSchema);
    mapMergeRecord.put(WriteComputeConstants.MAP_UNION, toAdd);
    mapMergeRecord.put(WriteComputeConstants.MAP_DIFF, toRemoveKeys);
    return mapMergeRecord;
  }
}
