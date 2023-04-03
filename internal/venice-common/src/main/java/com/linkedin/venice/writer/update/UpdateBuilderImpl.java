package com.linkedin.venice.writer.update;

import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaAdapter;
import com.linkedin.venice.schema.writecompute.WriteComputeConstants;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Validate;


@NotThreadsafe
@Experimental
public class UpdateBuilderImpl implements UpdateBuilder {
  private final GenericRecord updateRecord;
  private final RecordSerializer<GenericRecord> serializer;
  private final Set<String> updateFieldNameSet;
  private final Set<String> collectionMergeFieldNameSet;

  /**
   * @param updateSchema Update schema that is derived from the value Record schema.
   */
  public UpdateBuilderImpl(Schema updateSchema) {
    validateUpdateSchema(updateSchema);
    this.updateRecord = new GenericData.Record(updateSchema);
    this.serializer = FastSerializerDeserializerFactory.getAvroGenericSerializer(updateSchema);
    this.updateFieldNameSet = new HashSet<>();
    this.collectionMergeFieldNameSet = new HashSet<>();
  }

  private void validateUpdateSchema(Schema updateSchema) {
    if (updateSchema.getType() != Schema.Type.RECORD
        || !updateSchema.getName().endsWith(WriteComputeConstants.WRITE_COMPUTE_RECORD_SCHEMA_SUFFIX)) {
      throw new IllegalArgumentException("Got invalid record-update schema: " + updateSchema);
    }
  }

  @Override
  public UpdateBuilder setNewFieldValue(String fieldName, Object newFieldValue) {
    validateNoCollectionMergeOnField(fieldName);
    updateRecord.put(
        fieldName,
        SchemaAdapter.adaptToSchema(updateRecord.getSchema().getField(fieldName).schema(), newFieldValue));
    updateFieldNameSet.add(fieldName);
    return this;
  }

  @Override
  public UpdateBuilder setElementsToAddToListField(String listFieldName, List<?> elementsToAdd) {
    validateFieldType(Validate.notNull(listFieldName), Schema.Type.ARRAY);
    validateFieldNotSet(listFieldName);
    if (!elementsToAdd.isEmpty()) {
      getOrCreateListMergeRecord(listFieldName).put(
          WriteComputeConstants.SET_UNION,
          SchemaAdapter.adaptToSchema(getCorrespondingValueFieldSchema(listFieldName), elementsToAdd));
      collectionMergeFieldNameSet.add(listFieldName);
    }
    return this;
  }

  @Override
  public UpdateBuilder setElementsToRemoveFromListField(String listFieldName, List<?> elementsToRemove) {
    validateFieldType(Validate.notNull(listFieldName), Schema.Type.ARRAY);
    validateFieldNotSet(listFieldName);
    if (!elementsToRemove.isEmpty()) {
      getOrCreateListMergeRecord(listFieldName).put(WriteComputeConstants.SET_DIFF, elementsToRemove);
      collectionMergeFieldNameSet.add(listFieldName);
    }
    return this;
  }

  @Override
  public UpdateBuilder setEntriesToAddToMapField(String mapFieldName, Map<String, ?> entriesToAdd) {
    validateFieldType(Validate.notNull(mapFieldName), Schema.Type.MAP);
    validateFieldNotSet(mapFieldName);
    if (!entriesToAdd.isEmpty()) {
      getOrCreateMapMergeRecord(mapFieldName).put(
          WriteComputeConstants.MAP_UNION,
          SchemaAdapter.adaptToSchema(getCorrespondingValueFieldSchema(mapFieldName), entriesToAdd));
      collectionMergeFieldNameSet.add(mapFieldName);
    }
    return this;
  }

  @Override
  public UpdateBuilder setKeysToRemoveFromMapField(String mapFieldName, List<String> keysToRemove) {
    validateFieldType(Validate.notNull(mapFieldName), Schema.Type.MAP);
    validateFieldNotSet(mapFieldName);
    if (!keysToRemove.isEmpty()) {
      getOrCreateMapMergeRecord(mapFieldName).put(WriteComputeConstants.MAP_DIFF, keysToRemove);
      collectionMergeFieldNameSet.add(mapFieldName);
    }
    return this;
  }

  @Override
  public GenericRecord build() {
    if (updateFieldNameSet.isEmpty() && collectionMergeFieldNameSet.isEmpty()) {
      throw new IllegalStateException(
          "No update has been specified. Please use setter methods to specify how to partially "
              + "update a value record before calling this build method.");
    }
    for (Schema.Field updateField: updateRecord.getSchema().getFields()) {
      final String fieldName = updateField.name();
      if (updateFieldNameSet.contains(fieldName) || collectionMergeFieldNameSet.contains(fieldName)) {
        continue;
      }
      // This field has no field set and no collection merge.
      updateRecord.put(fieldName, createNoOpRecord(updateField));
    }
    Exception serializationException = validateUpdateRecordIsSerializable(updateRecord);
    if (serializationException != null) {
      throw new VeniceException(
          "The built partial-update record failed to be serialized. It could be caused by setting "
              + "field value(s) with wrong type(s). Built record: " + updateRecord + ", and serialization exception: ",
          serializationException);
    }
    return updateRecord;
  }

  private Exception validateUpdateRecordIsSerializable(GenericRecord updateRecord) {
    try {
      serializer.serialize(updateRecord);
    } catch (Exception serializationException) {
      return serializationException;
    }
    return null;
  }

  /**
   * Given a field from the partial update schema and find the schema of its corresponding value field.
   *
   * @param updateField A field from the partial update schema.
   * @return Schema of its corresponding value field.
   */
  private Schema getCorrespondingValueFieldSchema(String updateFieldName) {
    return getCorrespondingValueFieldSchema(updateRecord.getSchema().getField(updateFieldName));
  }

  /**
   * Given a field from the partial update schema and find the schema of its corresponding value field.
   *
   * @param updateField A field from the partial update schema.
   * @return Schema of its corresponding value field.
   */
  private Schema getCorrespondingValueFieldSchema(Schema.Field updateField) {
    // Each field in partial update schema is a union of multiple schemas.
    List<Schema> updateFieldSchemas = updateField.schema().getTypes();
    // The last schema in the union is the schema of the field in the corresponding value schema.
    return updateFieldSchemas.get(updateFieldSchemas.size() - 1);
  }

  /**
   * Given a field from the partial update schema and find the type of its corresponding value field.
   *
   * @param updateField A field from the partial update schema.
   * @return Type of its corresponding value field.
   */
  private Schema.Type getCorrespondingValueFieldType(Schema.Field updateField) {
    return getCorrespondingValueFieldSchema(updateField).getType();
  }

  private void validateFieldNotSet(String fieldName) {
    if (updateFieldNameSet.contains(fieldName)) {
      throw new IllegalStateException(
          "Field " + fieldName + " has already been set with value: " + updateRecord.get(fieldName));
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
    if (collectionMergeFieldNameSet.contains(fieldName)) {
      throw new IllegalStateException("Field " + fieldName + " already had collection merge element(s).");
    }
  }

  private GenericRecord createNoOpRecord(Schema.Field updateField) {
    // Because of the way how partial update schema is generated, the first entry in the every union write compute field
    // must be the no-op schema.
    Schema noOpRecordSchema = updateField.schema().getTypes().get(0);
    return new GenericData.Record(noOpRecordSchema);
  }

  // Caller of this method must check the field is a list field and is not set.
  private GenericRecord getOrCreateListMergeRecord(String listFieldName) {
    if (updateRecord.get(listFieldName) != null) {
      return (GenericRecord) updateRecord.get(listFieldName);
    }
    Schema listMergeRecordSchema = null;
    for (Schema unionBranchSchema: updateRecord.getSchema().getField(listFieldName).schema().getTypes()) {
      if (unionBranchSchema.getType().equals(Schema.Type.RECORD) && !unionBranchSchema.getFields().isEmpty()) {
        listMergeRecordSchema = unionBranchSchema;
        break;
      }
    }
    GenericRecord listMergeRecord = new GenericData.Record(listMergeRecordSchema);
    listMergeRecord.put(WriteComputeConstants.SET_UNION, Collections.emptyList());
    listMergeRecord.put(WriteComputeConstants.SET_DIFF, Collections.emptyList());
    updateRecord.put(listFieldName, listMergeRecord);
    return listMergeRecord;
  }

  // Caller of this method must check the field is a map field and is not set.
  private GenericRecord getOrCreateMapMergeRecord(String mapFieldName) {
    if (updateRecord.get(mapFieldName) != null) {
      return (GenericRecord) updateRecord.get(mapFieldName);
    }
    Schema mapMergeRecordSchema = null;
    for (Schema unionBranchSchema: updateRecord.getSchema().getField(mapFieldName).schema().getTypes()) {
      if (unionBranchSchema.getType().equals(Schema.Type.RECORD) && !unionBranchSchema.getFields().isEmpty()) {
        mapMergeRecordSchema = unionBranchSchema;
        break;
      }
    }
    GenericRecord mapMergeRecord = new GenericData.Record(mapMergeRecordSchema);
    mapMergeRecord.put(WriteComputeConstants.MAP_UNION, Collections.emptyMap());
    mapMergeRecord.put(WriteComputeConstants.MAP_DIFF, Collections.emptyList());
    updateRecord.put(mapFieldName, mapMergeRecord);
    return mapMergeRecord;
  }
}
