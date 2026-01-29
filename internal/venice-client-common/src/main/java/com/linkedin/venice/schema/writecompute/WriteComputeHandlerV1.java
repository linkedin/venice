package com.linkedin.venice.schema.writecompute;

import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.MAP_UNION;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_DIFF;
import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.SET_UNION;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.LIST_OPS;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.MAP_OPS;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.NO_OP_ON_FIELD;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.PUT_NEW_FIELD;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.getFieldOperationType;

import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.ArrayList;
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

    final GenericRecord updatedValue = currValue == null ? AvroSchemaUtils.createGenericRecord(valueSchema) : currValue;
    for (Schema.Field valueField: updatedValue.getSchema().getFields()) {
      final String valueFieldName = valueField.name();
      Object writeComputeFieldValue = writeComputeRecord.get(valueFieldName);
      if (isNoOpField(writeComputeFieldValue)) {
        // Skip updating this field if its Write Compute operation is NoOp

      } else {
        Object updatedFieldObject =
            updateFieldValue(valueField.schema(), updatedValue.get(valueField.pos()), writeComputeFieldValue);
        updatedValue.put(valueField.pos(), updatedFieldObject);
      }
    }
    return updatedValue;
  }

  @Override
  public GenericRecord mergeUpdateRecord(
      Schema valueSchema,
      GenericRecord currUpdateRecord,
      GenericRecord newUpdateRecord) {
    if (currUpdateRecord == null) {
      return newUpdateRecord;
    }
    if (!WriteComputeOperation.isPartialUpdateOp(currUpdateRecord)) {
      throw new IllegalStateException("Got unexpected update record: " + currUpdateRecord);
    }

    if (!WriteComputeOperation.isPartialUpdateOp(newUpdateRecord)) {
      throw new IllegalStateException("Got unexpected update record: " + currUpdateRecord);
    }
    for (Schema.Field valueField: currUpdateRecord.getSchema().getFields()) {
      final String valueFieldName = valueField.name();
      Object currentFieldValue = currUpdateRecord.get(valueFieldName);
      Object newFieldValue = newUpdateRecord.get(valueFieldName);

      if (isNoOpField(currentFieldValue)) {
        currUpdateRecord.put(valueField.pos(), newFieldValue);
        continue;
      }
      if (isNoOpField(newFieldValue)) {
        continue;
      }
      Object updatedFieldObject =
          mergeFieldUpdate(valueSchema.getField(valueFieldName).schema(), currentFieldValue, newFieldValue);
      currUpdateRecord.put(valueField.pos(), updatedFieldObject);
    }
    return currUpdateRecord;
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

  private Object mergeFieldUpdate(Schema valueFieldSchema, Object currFieldUpdate, Object newFieldUpdate) {
    switch (valueFieldSchema.getType()) {
      case ARRAY:
        return mergeArray(valueFieldSchema, currFieldUpdate, newFieldUpdate);
      case MAP:
        return mergeMap(currFieldUpdate, newFieldUpdate);
      case UNION:
        return mergeUnion(valueFieldSchema, currFieldUpdate, newFieldUpdate);
      default:
        return newFieldUpdate;
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

  Object mergeArray(Schema arraySchema, Object currArrayUpdate, Object newArrayUpdate) {
    if (getFieldOperationType(newArrayUpdate).equals(PUT_NEW_FIELD)) {
      return newArrayUpdate; // Partial update on a list field
    }
    /**
     * If old array update is {@link LIST_OPS}, merge it into the new collection merge OP's {@link SET_UNION} field.
     * If old array update is NULL, a new empty array is created to take in the new collection merge.
     * Otherwise, they are both {@link LIST_OPS}, just merge both {@link SET_UNION} field and {@link SET_DIFF} field.
     */
    List newSetUnion = (List) ((GenericRecord) newArrayUpdate).get(SET_UNION);
    List newSetDiff = (List) ((GenericRecord) newArrayUpdate).get(SET_DIFF);
    if (getFieldOperationType(currArrayUpdate).equals(LIST_OPS)) {
      List currSetUnion = (List) ((GenericRecord) currArrayUpdate).get(SET_UNION);
      List currSetDiff = (List) ((GenericRecord) currArrayUpdate).get(SET_DIFF);
      // Creating a new updated list to avoid the case that old list is unmodifiable.
      List updatedSetUnion = new GenericData.Array(arraySchema, newSetUnion);
      List updatedSetDiff = new GenericData.Array(arraySchema, newSetDiff);
      for (Object element: currSetUnion) {
        if (!updatedSetUnion.contains(element)) {
          updatedSetUnion.add(element);
        }
      }
      for (Object elementToRemove: currSetDiff) {
        if (!updatedSetDiff.contains(elementToRemove) && !updatedSetUnion.contains(elementToRemove)) {
          updatedSetDiff.add(elementToRemove);
        }
      }
      ((GenericRecord) newArrayUpdate).put(SET_UNION, updatedSetUnion);
      ((GenericRecord) newArrayUpdate).put(SET_DIFF, updatedSetDiff);

      return newArrayUpdate;
    } else {
      List updatedArray = new ArrayList<>();
      if (currArrayUpdate != null) {
        for (Object element: (List) currArrayUpdate) {
          if (!newSetDiff.contains(element)) {
            updatedArray.add(element);
          }
        }
      }
      for (Object element: newSetUnion) {
        if (!updatedArray.contains(element) && !newSetDiff.contains(element)) {
          updatedArray.add(element);
        }
      }
      return updatedArray;
    }
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

  Object mergeMap(Object currMapUpdate, Object newMapUpdate) {
    if (getFieldOperationType(newMapUpdate).equals(PUT_NEW_FIELD)) {
      return newMapUpdate; // Partial update on a map field
    }
    /**
     * If old map update is {@link MAP_OPS}, merge it into the new collection merge OP's {@link MAP_UNION} field.
     * If old map update is NULL, a new empty map is created to take in the new collection merge.
     * Otherwise, they are both {@link MAP_OPS}, just merge both {@link MAP_UNION} field and {@link MAP_DIFF} field.
     */
    Map newMapUnion = ((Map) ((GenericRecord) newMapUpdate).get(MAP_UNION));
    List newMapDiff = ((List) ((GenericRecord) newMapUpdate).get(MAP_DIFF));
    if (getFieldOperationType(currMapUpdate).equals(MAP_OPS)) {
      Map currMapUnion = ((Map) ((GenericRecord) currMapUpdate).get(MAP_UNION));
      List currMapDiff = ((List) ((GenericRecord) currMapUpdate).get(MAP_DIFF));
      List<Object> updatedMapDiff = new ArrayList<>();
      for (Object elementToRemove: currMapDiff) {
        if (!newMapDiff.contains(elementToRemove) && !newMapUnion.containsKey(elementToRemove)) {
          updatedMapDiff.add(elementToRemove);
        }
      }
      for (Object elementToRemove: newMapDiff) {
        if (!updatedMapDiff.contains(elementToRemove)) {
          updatedMapDiff.add(elementToRemove);
        }
      }
      for (Object entry: currMapUnion.entrySet()) {
        Object key = ((Map.Entry) entry).getKey();
        Object value = ((Map.Entry) entry).getValue();
        newMapUnion.putIfAbsent(key, value);
      }
      ((GenericRecord) newMapUpdate).put(MAP_DIFF, updatedMapDiff);
      return newMapUpdate;
    } else {
      Map updatedMap = currMapUpdate == null ? new HashMap<>() : (Map) currMapUpdate;
      for (Object entry: newMapUnion.entrySet()) {
        Object key = ((Map.Entry) entry).getKey();
        Object value = ((Map.Entry) entry).getValue();
        updatedMap.put(key, value);
      }
      for (Object diffKey: newMapDiff) {
        updatedMap.remove(diffKey);
      }
      return updatedMap;
    }
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

  /**
   * If original field is a union field, it will try to locate the collection field and perform collection merge.
   * If there is no collection field, or the new update on the collection field is a set field, it will directly return
   * the new update.
   */
  Object mergeUnion(Schema fieldValueSchema, Object currUpdateObject, Object newUpdateObject) {
    for (Schema subSchema: fieldValueSchema.getTypes()) {
      if (subSchema.getType() == Schema.Type.ARRAY && newUpdateObject instanceof IndexedRecord
          && ((IndexedRecord) newUpdateObject).getSchema().getName().endsWith(LIST_OPS.name)) {
        return mergeArray(subSchema, currUpdateObject, newUpdateObject);
      }

      if (subSchema.getType() == Schema.Type.MAP && newUpdateObject instanceof IndexedRecord
          && ((IndexedRecord) newUpdateObject).getSchema().getName().endsWith(MAP_OPS.name)) {
        return mergeMap(currUpdateObject, newUpdateObject);
      }
    }
    return newUpdateObject;
  }
}
