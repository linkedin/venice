package com.linkedin.venice.schema.writecompute;

import com.linkedin.venice.schema.SchemaUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import static com.linkedin.venice.schema.writecompute.WriteComputeConstants.*;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.*;


/**
 * Write compute V1 handles value records that do not have replication metadata.
 */
public class WriteComputeHandlerV1 implements WriteComputeHandler {
  // GenericData is a singleton util class Avro provides. We're using it to construct the default field values
  protected static final GenericData genericData = GenericData.get();

  @Override
  public GenericRecord updateRecord(Schema originalSchema, Schema writeComputeSchema, GenericRecord originalRecord, GenericRecord writeComputeRecord) {
    return updateRecord(originalSchema, writeComputeSchema, originalRecord, writeComputeRecord, false);
  }

  GenericRecord updateRecord(Schema originalSchema, Schema writeComputeSchema, GenericRecord originalRecord,
      GenericRecord writeComputeRecord, boolean nestedField) {
    if (nestedField) {
      return writeComputeRecord;
    }

    // Question: maybe we do not need to check if writeComputeSchema is an UNION at all here??
    if (originalSchema.getType() == Schema.Type.RECORD && writeComputeSchema.getType() == Schema.Type.UNION) {
      //if DEL_OP is in writeComputeRecord, return empty record
      if (WriteComputeOperation.isDeleteRecordOp(writeComputeRecord)) {
        return null;
      } else {
        return updateRecord(
            originalSchema,
            writeComputeSchema.getTypes().get(0),
            originalRecord == null ? SchemaUtils.constructGenericRecord(originalSchema) : originalRecord,
            writeComputeRecord,
            false
        );
      }
    }

    if (originalRecord == null) {
      originalRecord = SchemaUtils.constructGenericRecord(originalSchema);
    }
    for (Schema.Field originalField : originalSchema.getFields()) {
      final String originalFieldName = originalField.name();
      Object writeComputeFieldValue = writeComputeRecord.get(originalFieldName);

      //skip the fields if it's NoOp
      if (writeComputeFieldValue instanceof IndexedRecord &&
          ((IndexedRecord) writeComputeFieldValue).getSchema().getName().equals(NO_OP_ON_FIELD.name)) {
        continue;
      }

      Object updatedFieldObject = update(originalField.schema(), writeComputeSchema.getField(originalFieldName).schema(),
          originalRecord.get(originalFieldName), writeComputeFieldValue);
      originalRecord.put(originalFieldName, updatedFieldObject);
    }

    return originalRecord;
  }

  /**
   * Apply write compute updates recursively.
   * @param originalSchema the original schema that write compute schema is derived from
   * @param originalValue current value before write compute updates are applied. Notice that this is nullable.
   *                      A key can be nonexistent in the DB or a field is designed to be nullable. In this
   *                      case, Venice will create an empty record/field and apply the updates
   * @param writeComputeValue write-computed value that is going to be applied
   *                          on top of original value.
   * @return The updated value
   */
  private Object update(Schema originalSchema, Schema writeComputeSchema, Object originalValue,
      Object writeComputeValue) {
    switch (originalSchema.getType()) {
      case RECORD:
        return updateRecord(originalSchema, writeComputeSchema, (GenericRecord) originalValue,
            (GenericRecord) writeComputeValue, true);
      case ARRAY:
        return updateArray(originalSchema, (List) originalValue, writeComputeValue);
      case MAP:
        return updateMap((Map) originalValue, writeComputeValue);
      case UNION:
        return updateUnion(originalSchema, originalValue, writeComputeValue);
      default:
        return writeComputeValue;
    }
  }

  // Visible for testing
  Object updateArray(Schema arraySchema, List originalArray, Object writeComputeArray) {
    if (writeComputeArray instanceof List) {
      return writeComputeArray; // Partial update on a list field
    }

    //if originalArray is null, use the elements in the "setUnion" as the base list to conduct collection merging.
    List newElements = (List) ((GenericRecord) writeComputeArray).get(SET_UNION);
    if (originalArray == null) {
      originalArray = new GenericData.Array(arraySchema, newElements);
    } else {
      for (Object element : newElements) {
        if (!(originalArray).contains(element)) { //TODO: profile the performance since this is pretty expensive
          originalArray.add(element);
        }
      }
    }

    for (Object elementToRemove : (List) ((GenericRecord) writeComputeArray).get(SET_DIFF)) {
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
      for (Object entry : newEntries.entrySet()) {
        originalMap.put(((Map.Entry) entry).getKey(), ((Map.Entry) entry).getValue());
      }
    }

    for (Object key : (List) ((GenericRecord) writeComputeMap).get(MAP_DIFF)) {
      originalMap.remove(key);
    }

    return originalMap;
  }

  // Visible for testing
  Object updateUnion(Schema originalSchema, Object originalObject, Object writeComputeObject) {
    for (Schema subSchema : originalSchema.getTypes()) {
      if (subSchema.getType() == Schema.Type.ARRAY && writeComputeObject instanceof IndexedRecord &&
          ((IndexedRecord) writeComputeObject).getSchema().getName().endsWith(LIST_OPS.name)) {
        return updateArray(subSchema, (List) originalObject, writeComputeObject);
      }

      if (subSchema.getType() == Schema.Type.MAP && writeComputeObject instanceof IndexedRecord &&
          ((IndexedRecord) writeComputeObject).getSchema().getName().endsWith(MAP_OPS.name)) {
        return updateMap((Map) originalObject, writeComputeObject);
      }
    }

    // TODO: There is an edge case that is not handled and that is when writeComputeObject wants to do collection
    //       modification. But the original union schema does not contain List nor Map schema. That is an illegal state
    //       and should be caught and thrown.
    //       If the above situation happens, the field value becomes writeComputeObject which represents collection merging
    //       operation instead of a real field value.
    return writeComputeObject;
  }
}
