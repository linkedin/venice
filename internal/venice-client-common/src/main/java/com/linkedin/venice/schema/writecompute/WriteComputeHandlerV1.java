package com.linkedin.venice.schema.writecompute;

import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_UNION;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_UNION;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.LIST_OPS;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.MAP_OPS;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.NO_OP_ON_FIELD;

import com.linkedin.venice.schema.SchemaUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;


/**
 * Write compute V1 handles value records that do not have replication metadata.
 */
public class WriteComputeHandlerV1 implements WriteComputeHandler {
  // GenericData is a singleton util class Avro provides. We're using it to construct the default field values
  protected static final GenericData GENERIC_DATA = GenericData.get();

  @Override
  public GenericRecord updateValueRecord(
      Schema valueSchema,
      GenericRecord currValue,
      GenericRecord writeComputeRecord) {
    if (valueSchema.getType() != Schema.Type.RECORD) {
      throw new IllegalStateException("Expect a Record value schema. Got: " + valueSchema);
    }
    if (!WriteComputeOperation.isPartialUpdateOp(writeComputeRecord)) {
      // This Write Compute record could be a Write Compute Delete request which is not supported and there should be no
      // one using it.
      throw new IllegalStateException(
          "Write Compute only support partial update. Got unexpected Write Compute record: " + writeComputeRecord);
    }

    final GenericRecord updatedValue = currValue == null ? SchemaUtils.createGenericRecord(valueSchema) : currValue;
    for (Schema.Field valueField: valueSchema.getFields()) {
      final String valueFieldName = valueField.name();
      Object writeComputeFieldValue = writeComputeRecord.get(valueFieldName);
      if (isNoOpField(writeComputeFieldValue)) {
        // Skip updating this field if its Write Compute operation is NoOp

      } else {
        Object updatedFieldObject =
            updateFieldValue(valueField.schema(), updatedValue.get(valueFieldName), writeComputeFieldValue);
        updatedValue.put(valueFieldName, updatedFieldObject);
      }
    }
    return updatedValue;
  }

  private boolean isNoOpField(Object writeComputeFieldValue) {
    return (writeComputeFieldValue instanceof IndexedRecord)
        && ((IndexedRecord) writeComputeFieldValue).getSchema().getName().equals(NO_OP_ON_FIELD.name);
  }

  /**
   * Apply write compute updates on a field in a value record.
   * @param valueFieldSchema the schema for this field.
   * @param originalFieldValue current field value before it gets updated. Note that this is nullable.
   *                      A key can be nonexistent in the DB or a field is designed to be nullable. In this
   *                      case, Venice will create an empty record/field and apply the updates
   * @param writeComputeFieldValue write-computed value that is going to be applied
   *                          on top of original value.
   * @return The updated value
   */
  private Object updateFieldValue(Schema valueFieldSchema, Object originalFieldValue, Object writeComputeFieldValue) {
    switch (valueFieldSchema.getType()) {
      case ARRAY:
        return updateArray(valueFieldSchema, (List) originalFieldValue, writeComputeFieldValue);
      case MAP:
        return updateMap((Map) originalFieldValue, writeComputeFieldValue);
      case UNION:
        return updateUnion(valueFieldSchema, originalFieldValue, writeComputeFieldValue);
      default:
        return writeComputeFieldValue;
    }
  }

  // Visible for testing
  Object updateArray(Schema arraySchema, List originalArray, Object writeComputeArray) {
    if (writeComputeArray instanceof List) {
      return writeComputeArray; // Partial update on a list field
    }

    // if originalArray is null, use the elements in the "setUnion" as the base list to conduct collection merging.
    List newElements = (List) ((GenericRecord) writeComputeArray).get(SET_UNION);
    if (originalArray == null) {
      originalArray = new GenericData.Array(arraySchema, newElements);
    } else {
      for (Object element: newElements) {
        if (!(originalArray).contains(element)) { // TODO: profile the performance since this is pretty expensive
          originalArray.add(element);
        }
      }
    }

    for (Object elementToRemove: (List) ((GenericRecord) writeComputeArray).get(SET_DIFF)) {
      // We need to iterate the list by our own because #remove(T object) is not supported by GenericRecord.Array
      for (int i = 0; i < originalArray.size(); i++) {
        if (originalArray.get(i).equals(elementToRemove)) {
          originalArray.remove(i);
          break;
        }
      }
    }

    return originalArray;
  }

  // Visible for testing
  Object updateMap(Map originalMap, Object writeComputeMap) {
    if (writeComputeMap instanceof Map) {
      return writeComputeMap; // Partial update on a map field
    }

    // Conduct collection merging
    Map newEntries = ((Map) ((GenericRecord) writeComputeMap).get(MAP_UNION));
    if (originalMap == null) {
      originalMap = new HashMap(newEntries);
    } else {
      for (Object entry: newEntries.entrySet()) {
        originalMap.put(((Map.Entry) entry).getKey(), ((Map.Entry) entry).getValue());
      }
    }

    for (Object key: (List) ((GenericRecord) writeComputeMap).get(MAP_DIFF)) {
      originalMap.remove(key);
    }

    return originalMap;
  }

  // Visible for testing
  Object updateUnion(Schema originalSchema, Object originalObject, Object writeComputeObject) {
    for (Schema subSchema: originalSchema.getTypes()) {
      if (subSchema.getType() == Schema.Type.ARRAY && writeComputeObject instanceof IndexedRecord
          && ((IndexedRecord) writeComputeObject).getSchema().getName().endsWith(LIST_OPS.name)) {
        return updateArray(subSchema, (List) originalObject, writeComputeObject);
      }

      if (subSchema.getType() == Schema.Type.MAP && writeComputeObject instanceof IndexedRecord
          && ((IndexedRecord) writeComputeObject).getSchema().getName().endsWith(MAP_OPS.name)) {
        return updateMap((Map) originalObject, writeComputeObject);
      }
    }

    // TODO: There is an edge case that is not handled and that is when writeComputeObject wants to do collection
    // modification. But the original union schema does not contain List nor Map schema. That is an illegal state
    // and should be caught and thrown.
    // If the above situation happens, the field value becomes writeComputeObject which represents collection merging
    // operation instead of a real field value.
    return writeComputeObject;
  }
}
