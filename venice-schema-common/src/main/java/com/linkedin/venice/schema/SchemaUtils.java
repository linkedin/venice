package com.linkedin.venice.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static org.apache.avro.Schema.Type.*;


public class SchemaUtils {
  /**
   * Utility function that checks to make sure that given a union schema, there only exists 1 collection type amongst the
   * provided types.  Multiple collections will make the result of the flattened write compute schema lead to ambiguous behavior
   *
   * @param unionSchema a union schema to validate.
   * @throws VeniceException When the unionSchema contains more then one collection type
   */
  public static void containsOnlyOneCollection(Schema unionSchema) {
    List<Schema> types = unionSchema.getTypes();
    boolean hasCollectionType = false;
    for (Schema type : types) {
      switch (type.getType()) {
        case ARRAY:
        case MAP:
          if (hasCollectionType) {
            // More then one collection type found, this won't work.
            throw new VeniceException("Multiple collection types in a union are not allowedSchema: "
                + unionSchema.toString(true));
          }
          hasCollectionType = true;
          continue;
        case RECORD:
        case UNION:
        default:
          continue;
      }
    }
  }

  /**
   * @param unionSchema
   * @return True iif the schema is of type UNION and it has 2 fields and one of them is NULL.
   */
  public static boolean isNullableUnionPair(Schema unionSchema) {
    if (unionSchema.getType() != Schema.Type.UNION) {
      return false;
    }
    List<Schema> types = unionSchema.getTypes();
    if (types.size() != 2) {
      return false;
    }

    return types.get(0).getType() == Schema.Type.NULL
        || types.get(1).getType() == Schema.Type.NULL;
  }

  public static Schema createFlattenedUnionSchema(List<Schema> schemasInUnion) {
    List<Schema> flattenedSchemaList = new ArrayList<>(schemasInUnion.size());
    for (Schema schemaInUnion : schemasInUnion) {
      //if the origin schema is union, we'd like to flatten it
      //we don't need to do it recursively because Avro doesn't support nested union
      if (schemaInUnion.getType() == UNION) {
        flattenedSchemaList.addAll(schemaInUnion.getTypes());
      } else {
        flattenedSchemaList.add(schemaInUnion);
      }
    }

    return Schema.createUnion(flattenedSchemaList);
  }

  public static GenericRecord constructGenericRecord(Schema originalSchema) {
    final GenericData.Record newRecord = new GenericData.Record(originalSchema);

    for (Schema.Field originalField : originalSchema.getFields()) {
      if (AvroCompatibilityHelper.fieldHasDefault(originalField)) {
        //make a deep copy here since genericData caches each default value internally. If we
        //use what it returns, we will mutate the cache.
        newRecord.put(originalField.name(), GenericData.get().deepCopy(originalField.schema(), AvroCompatibilityHelper.getGenericDefaultValue(originalField)));
      } else {
        throw new VeniceException(String.format("Cannot apply updates because Field: %s is null and "
            + "default value is not defined", originalField.name()));
      }
    }

    return newRecord;
  }
}
