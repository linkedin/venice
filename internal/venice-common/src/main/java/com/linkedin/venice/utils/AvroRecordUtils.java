package com.linkedin.venice.utils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;


public class AvroRecordUtils {
  private AvroRecordUtils() {
  }

  /**
   * This function is used to pre-fill the default value defined in schema.
   * So far, it only support the followings:
   * 1. Only top-level fields are supported.
   * 2. All the primitive types are supported.
   * 3. For union, only `null` default is supported.
   * 4. For array/map, only empty list/map are supported.
   *
   * Other than the above, this function will throw exception.
   *
   * TODO: once the whole stack migrates to the modern avro version (avro-1.7+), we could leverage
   * SpecificRecord builder to prefill the default value, which will be much more powerful.
   * @param recordType
   * @param <T>
   * @return
   */
  public static <T extends SpecificRecord> T prefillAvroRecordWithDefaultValue(T recordType) {
    Schema schema = recordType.getSchema();
    for (Schema.Field field: schema.getFields()) {
      if (AvroCompatibilityHelper.fieldHasDefault(field)) {
        // has default
        Object defaultValue = AvroCompatibilityHelper.getSpecificDefaultValue(field);
        Schema.Type fieldType = field.schema().getType();
        switch (fieldType) {
          case NULL:
            throw new VeniceException("Default value for `null` type is not expected");
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
            recordType.put(field.pos(), defaultValue);
            break;
          case UNION:
            if (defaultValue == null) {
              recordType.put(field.pos(), null);
            } else {
              throw new VeniceException("Non 'null' default value is not supported for union type: " + field.name());
            }
            break;
          case ARRAY:
            Collection collection = (Collection) defaultValue;
            if (collection.isEmpty()) {
              recordType.put(field.pos(), new ArrayList<>());
            } else {
              throw new VeniceException(
                  "Non 'empty array' default value is not supported for array type: " + field.name());
            }
            break;
          case MAP:
            Map map = (Map) defaultValue;
            if (map.isEmpty()) {
              recordType.put(field.pos(), new HashMap<>());
            } else {
              throw new VeniceException("Non 'empty map' default value is not supported for map type: " + field.name());
            }
            break;
          case ENUM:
          case FIXED:
          case BYTES:
          case RECORD:
            throw new VeniceException(
                "Default value for field: " + field.name() + " with type: " + fieldType + " is not supported");
        }
      }
    }

    return recordType;
  }

  public static void clearRecord(GenericRecord record) {
    if (record instanceof SpecificRecord) {
      throw new UnsupportedOperationException("SpecificRecord clearing is not supported");
    }
    int fieldCount = record.getSchema().getFields().size();
    for (int i = 0; i < fieldCount; ++i) {
      record.put(i, null);
    }
  }
}
