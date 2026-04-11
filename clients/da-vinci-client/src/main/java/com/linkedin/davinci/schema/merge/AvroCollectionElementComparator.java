package com.linkedin.davinci.schema.merge;

import com.linkedin.davinci.utils.IndexedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;


/**
 * This comparator is used to compare GenericRecord collection field elements which must have schemas. This comparison
 * is required in order to determine orders of 2 collection elements when their timestamps are the same.
 */
@ThreadSafe
public class AvroCollectionElementComparator {
  public final static AvroCollectionElementComparator INSTANCE = new AvroCollectionElementComparator();

  private AvroCollectionElementComparator() {
    // Singleton class.
  }

  /**
   * This function compares two objects using the provided schema.
   * @param o1 first object to compare
   * @param o2 second object to compare
   * @param schema Schema of o1 and o2. Used to determine their type for the comparison
   * @return a negative integer if o1 is less than o2, zero if o1 is equal to o2, or a positive integer if o1 is greater than o2.
   *         When o1 and o2 are IndexedHashMaps:
   *          returns a negative integer if o1.size() < o2.size(), a positive integer if o1.size() > o2.size, or a result of
   *          entry-by-entry comparison which is done using positional indexes.
   * @throws IllegalArgumentException if o1 and o2 have different schemas
   */
  public int compare(Object o1, Object o2, Schema schema) {
    Validate.notNull(schema);
    if (o1 == o2) {
      return 0;
    }
    switch (schema.getType()) {
      case MAP:
        return compareMaps(validateAndCastToMapType(o1), validateAndCastToMapType(o2), schema);
      case RECORD:
        return compareRecords((GenericRecord) o1, (GenericRecord) o2, schema);
      case ARRAY:
        return compareArrays((List<?>) o1, (List<?>) o2, schema);
      case UNION:
        return compareUnion(o1, o2, schema);
      default:
        return GenericData.get().compare(o1, o2, schema);
    }
  }

  private int compareRecords(GenericRecord r1, GenericRecord r2, Schema schema) {
    for (Schema.Field field: schema.getFields()) {
      if (field.order() == Schema.Field.Order.IGNORE) {
        continue;
      }
      int pos = field.pos();
      int cmp = compare(r1.get(pos), r2.get(pos), field.schema());
      if (cmp != 0) {
        return field.order() == Schema.Field.Order.DESCENDING ? -cmp : cmp;
      }
    }
    return 0;
  }

  private int compareArrays(List<?> a1, List<?> a2, Schema arraySchema) {
    Schema elementSchema = arraySchema.getElementType();
    int minSize = Math.min(a1.size(), a2.size());
    for (int i = 0; i < minSize; i++) {
      int cmp = compare(a1.get(i), a2.get(i), elementSchema);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(a1.size(), a2.size());
  }

  private int compareUnion(Object o1, Object o2, Schema unionSchema) {
    int index1 = GenericData.get().resolveUnion(unionSchema, o1);
    int index2 = GenericData.get().resolveUnion(unionSchema, o2);
    if (index1 != index2) {
      return Integer.compare(index1, index2);
    }
    return compare(o1, o2, unionSchema.getTypes().get(index1));
  }

  private int compareMaps(IndexedHashMap<String, Object> map1, IndexedHashMap<String, Object> map2, Schema mapSchema) {
    if (map1 == map2) {
      return 0;
    }
    if (map1.size() != map2.size()) {
      return map1.size() > map2.size() ? 1 : -1;
    }
    Schema valueSchema = mapSchema.getValueType();
    boolean schemaCompared = false;

    // Same size
    for (int i = 0; i < map1.size(); i++) {
      Map.Entry<String, Object> entry1 = map1.getByIndex(i);
      Map.Entry<String, Object> entry2 = map2.getByIndex(i);

      final int keyCompareResult = entry1.getKey().compareTo(entry2.getKey());
      if (keyCompareResult != 0) {
        return keyCompareResult;
      }
      // Same key. So compare values and assume that every value has the same schema in a map.
      // For GenericContainer values, use the actual value schema to handle schema evolution.
      // For primitive values, use the map schema's value type.
      Schema effectiveValueSchema = valueSchema;
      if (entry1.getValue() instanceof GenericContainer) {
        effectiveValueSchema = ((GenericContainer) entry1.getValue()).getSchema();
        if (!schemaCompared) {
          Schema otherEntrySchema = ((GenericContainer) entry2.getValue()).getSchema();
          final int schemaCompareResult = compareSchemas(effectiveValueSchema, otherEntrySchema);
          if (schemaCompareResult == 0) {
            schemaCompared = true;
          } else {
            // Schemas are different in two maps.
            return schemaCompareResult;
          }
        }
      }

      final int compareValueResult = compare(entry1.getValue(), entry2.getValue(), effectiveValueSchema);
      if (compareValueResult != 0) {
        return compareValueResult;
      }
    }
    return 0; // All entries are the same.
  }

  private int compareSchemas(Schema schema1, Schema schema2) {
    if (schema1.equals(schema2)) {
      return 0;
    }
    Schema.Type type1 = schema1.getType();
    Schema.Type type2 = schema2.getType();
    if (type1 != type2) {
      return type1.compareTo(type2);
    }
    // Same schema type.
    int schemaStrCompareResult = schema1.toString().compareTo(schema2.toString());
    if (schemaStrCompareResult == 0) {
      // Assume this case is rare for now.
      throw new IllegalStateException(
          "TODO: handle this case where 2 schemas are not equal but their toString forms " + "equal. Schema string: "
              + schema1);
    } else {
      return schemaStrCompareResult;
    }
  }

  private IndexedHashMap<String, Object> validateAndCastToMapType(Object object) {
    if (!(object instanceof IndexedHashMap)) {
      throw new IllegalArgumentException("Expect IndexedHashMap. Got: " + object.getClass().getCanonicalName());
    }
    return (IndexedHashMap<String, Object>) object;
  }
}
