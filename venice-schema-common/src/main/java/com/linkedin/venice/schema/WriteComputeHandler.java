package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static com.linkedin.venice.schema.WriteComputeSchemaConverter.WriteComputeOperation.*;
import static com.linkedin.venice.schema.WriteComputeSchemaConverter.*;


/**
 * This class is able to read write-computed value and apply it to original value.
 *
 * Notice: though it's possible, we only support GenericRecord updating at this point. That's
 * being said, both original and write compute object need to be GenericRecord.
 *
 * Currently, Venice supports 2 kinds of updates.
 * 1. Record partial update.
 * 2. Collection merging.
 * TODO: since this class is very performance sensitive, we should add metrics to measure the
 * TODO: time it spends and keep optimizing the operations
 */
public class WriteComputeHandler {
  private final Schema originalSchema;
  private final Schema writeComputeSchema;

  //GenericData is a singleton util class Avro provides. We're using it to construct the default field values
  private final GenericData genericData = GenericData.get();

  /**
   * Generate a new write compute adapter that can be used given a pair of original schema and its write-compute
   * schema.
   * @param originalSchema the original schema that write compute schema is derived from
   * @param writeComputeSchema the write compute schema that is auto-generated and paired with original Schema.
   *                           See {@link WriteComputeSchemaConverter} for more details that how it's generated.
   */
  public static WriteComputeHandler getWriteComputeAdapter(Schema originalSchema, Schema writeComputeSchema) {
    WriteComputeSchemaValidator.validate(originalSchema, writeComputeSchema);

    return new WriteComputeHandler(originalSchema, writeComputeSchema);
  }

  WriteComputeHandler(Schema originalSchema, Schema writeComputeSchema) {
    this.originalSchema = originalSchema;
    this.writeComputeSchema = writeComputeSchema;
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

  public GenericRecord updateRecord(GenericRecord originalRecord, GenericRecord writeComputeRecord) {
    return updateRecord(originalSchema, writeComputeSchema, originalRecord, writeComputeRecord, false);
  }

  GenericRecord updateRecord(Schema originalSchema, Schema writeComputeSchema, GenericRecord originalRecord,
      GenericRecord writeComputeRecord, boolean nestedField) {
    if (nestedField) {
      return writeComputeRecord;
    }

    if (originalSchema.getType() == Schema.Type.RECORD && writeComputeSchema.getType() == Schema.Type.UNION) {
      //if DEL_OP is in writeComputeRecord, return empty record
      if (writeComputeRecord.getSchema().getName().equals(DEL_OP.name)) {
        return null;
      } else {
        return updateRecord(
            originalSchema,
            writeComputeSchema.getTypes().get(0),
            originalRecord == null ? constructNewRecord(originalSchema) : originalRecord,
            writeComputeRecord,
            false
        );
      }
    }

    if (originalRecord == null) {
      originalRecord = constructNewRecord(originalSchema);
    }
    for (Schema.Field originalField : originalSchema.getFields()) {
      final String originalFieldName = originalField.name();
      Object writeComputeFieldValue = writeComputeRecord.get(originalFieldName);

      //skip the fields if it's NoOp
      if (writeComputeFieldValue instanceof GenericRecord &&
          ((GenericRecord) writeComputeFieldValue).getSchema().getName().equals(NO_OP.name)) {
        continue;
      }

      Object updatedFieldObject = update(originalField.schema(), writeComputeSchema.getField(originalFieldName).schema(),
          originalRecord.get(originalFieldName), writeComputeFieldValue);
      originalRecord.put(originalFieldName, updatedFieldObject);
    }

    return originalRecord;
  }

  private GenericRecord constructNewRecord(Schema originalSchema) {
    final GenericData.Record newRecord = new GenericData.Record(originalSchema);

    for (Schema.Field originalField : originalSchema.getFields()) {
      if (originalField.defaultValue() != null) {
        //make a deep copy here since genericData caches each default value internally. If we
        //use what it returns, we will mutate the cache.
        newRecord.put(originalField.name(), genericData.deepCopy(originalField.schema(), genericData.getDefaultValue(originalField)));
      } else {
        throw new VeniceException(String.format("Cannot apply updates because Field: %s is null and "
            + "default value is not defined", originalField.name()));
      }
    }

    return newRecord;
  }

  // Visible for testing
  Object updateArray(Schema originalSchema, List originalArray, Object writeComputeArray) {
    if (writeComputeArray instanceof List) {
      return writeComputeArray;
    }

    //if originalArray is null, use the elements in the "setUnion" as the base list
    List newElements = (List) ((GenericRecord) writeComputeArray).get(SET_UNION);
    if (originalArray == null) {
      originalArray = new GenericData.Array(originalSchema, newElements);
    } else {
      for (Object element : newElements) {
        if (!(originalArray).contains(element)) { //TODO: profile the performance since this is pretty expensive
          originalArray.add(element);
        }
      }
    }

    for (Object elementToRemove : (List) ((GenericRecord) writeComputeArray).get(SET_DIFF)) {
      /**
       * we need to iterate the list by our own because #remove(T object) is not
       * supported by GenericRecord.Array
       */
      for (int i = 0; i < originalArray.size(); i ++) {
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
      return writeComputeMap;
    }

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
      if (subSchema.getType() == Schema.Type.ARRAY && writeComputeObject instanceof GenericRecord &&
          ((GenericRecord) writeComputeObject).getSchema().getName().endsWith(LIST_OPS.name)) {
        return updateArray(subSchema, (List) originalObject, writeComputeObject);
      }

      if (subSchema.getType() == Schema.Type.MAP && writeComputeObject instanceof GenericRecord &&
          ((GenericRecord) writeComputeObject).getSchema().getName().endsWith(MAP_OPS.name)) {
        return updateMap((Map) originalObject, writeComputeObject);
      }
    }

    return writeComputeObject;
  }
}
