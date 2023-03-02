package com.linkedin.venice.schema;

import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_FIELD_NAME;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class SchemaUtils {
  private SchemaUtils() {
    // Utility class
  }

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
    for (Schema type: types) {
      switch (type.getType()) {
        case ARRAY:
        case MAP:
          if (hasCollectionType) {
            // More then one collection type found, this won't work.
            throw new VeniceException(
                "Multiple collection types in a union are not allowedSchema: " + unionSchema.toString(true));
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

    return types.get(0).getType() == Schema.Type.NULL || types.get(1).getType() == Schema.Type.NULL;
  }

  public static Schema createFlattenedUnionSchema(List<Schema> schemasInUnion) {
    List<Schema> flattenedSchemaList = new ArrayList<>(schemasInUnion.size());
    for (Schema schemaInUnion: schemasInUnion) {
      // if the origin schema is union, we'd like to flatten it
      // we don't need to do it recursively because Avro doesn't support nested union
      if (schemaInUnion.getType() == UNION) {
        flattenedSchemaList.addAll(schemaInUnion.getTypes());
      } else {
        flattenedSchemaList.add(schemaInUnion);
      }
    }

    return Schema.createUnion(flattenedSchemaList);
  }

  /**
   * Create a {@link GenericRecord} from a given schema. The created record has default values set on all fields. Note
   * that all fields in the given schema must have default values. Otherwise, an exception is thrown.
   */
  public static GenericRecord createGenericRecord(Schema originalSchema) {
    final GenericData.Record newRecord = new GenericData.Record(originalSchema);
    for (Schema.Field originalField: originalSchema.getFields()) {
      if (AvroCompatibilityHelper.fieldHasDefault(originalField)) {
        // make a deep copy here since genericData caches each default value internally. If we
        // use what it returns, we will mutate the cache.
        newRecord.put(
            originalField.name(),
            GenericData.get()
                .deepCopy(originalField.schema(), AvroCompatibilityHelper.getGenericDefaultValue(originalField)));
      } else {
        throw new VeniceException(
            String.format(
                "Cannot apply updates because Field: %s is null and " + "default value is not defined",
                originalField.name()));
      }
    }

    return newRecord;
  }

  /**
   * Annotate all the top-level map field of the input schema to use Java String as key.
   * @param schema the input value schema to be annotated.
   * @return Annotated value schema.
   */
  public static Schema annotateStringMapInValueSchema(Schema schema) {
    // Create duplicate schema here in order not to create any side effect during annotation.
    Schema replicatedSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schema.toString());
    if (replicatedSchema.getType().equals(RECORD)) {
      for (Schema.Field field: replicatedSchema.getFields()) {
        if (field.schema().getType().equals(MAP)) {
          annotateMapSchema(field.schema());
        }
      }
    }
    return replicatedSchema;
  }

  /**
   * Annotate all the top-level map field of the partial update schema to use Java String as key. This method will make
   * sure field update and collection merging operations of a top-level map field are annotated.
   * @param schema the input partial update schema to be annotated.
   * @return Annotated partial update schema.
   */
  public static Schema annotateStringMapInPartialUpdateSchema(Schema schema) {
    // Create duplicate schema here in order not to create any side effect during annotation.
    Schema replicatedSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schema.toString());
    if (replicatedSchema.getType().equals(RECORD)) {
      for (Schema.Field field: replicatedSchema.getFields()) {
        if (field.schema().isUnion()) {
          for (Schema unionBranchSchema: field.schema().getTypes()) {
            // Full update request for Map field.
            if (unionBranchSchema.getType().equals(Schema.Type.MAP)) {
              annotateMapSchema(unionBranchSchema);
            } else if (unionBranchSchema.getType().equals(RECORD)) {
              for (Schema.Field updateOpField: unionBranchSchema.getFields()) {
                Schema.Type updateOpFieldType = updateOpField.schema().getType();
                if (updateOpFieldType.equals(MAP)) {
                  annotateMapSchema(updateOpField.schema());
                } else if (updateOpFieldType.equals(ARRAY)
                    && updateOpField.schema().getElementType().getType().equals(STRING)) {
                  annotateStringArraySchema(updateOpField.schema());
                }
              }
            }
          }
        }
      }
    }
    return replicatedSchema;
  }

  public static Schema annotateStringMapInRmdSchema(Schema schema) {
    // Create duplicate schema here in order not to create any side effect during annotation.
    Schema replicatedSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schema.toString());
    Schema fieldLevelTsSchema = replicatedSchema.getField(RmdConstants.TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    for (Schema.Field field: fieldLevelTsSchema.getFields()) {
      // For current RMD schema structure, there will be no union, adding this just for defensive coding.
      if (!field.schema().isUnion() && field.schema().getType().equals(RECORD)) {
        Schema.Field deletedElementField = field.schema().getField(DELETED_ELEM_FIELD_NAME);
        if (deletedElementField != null && deletedElementField.schema().getElementType().getType().equals(STRING)) {
          annotateStringArraySchema(deletedElementField.schema());
        }
      }
    }
    return replicatedSchema;
  }

  /**
   * Create a new {@link SchemaEntry} for value schema annotation.
   * @param schemaEntry Input {@link SchemaEntry}, containing the value schema to be annotated.
   * @return Annotated value schema in a newly created {@link SchemaEntry}
   */
  public static SchemaEntry getAnnotatedStringMapValueSchemaEntry(SchemaEntry schemaEntry) {
    if (schemaEntry == null) {
      return null;
    }
    Schema annotatedSchema = annotateStringMapInValueSchema(schemaEntry.getSchema());
    return new SchemaEntry(schemaEntry.getId(), annotatedSchema);
  }

  /**
   * Create a new {@link DerivedSchemaEntry} for partial update schema annotation.
   * @param schemaEntry Input {@link DerivedSchemaEntry}, containing the partial update schema to be annotated.
   * @return Annotated partial update schema in a newly created {@link DerivedSchemaEntry}
   */
  public static DerivedSchemaEntry getAnnotatedStringMapDerivedSchemaEntry(DerivedSchemaEntry schemaEntry) {
    if (schemaEntry == null) {
      return null;
    }
    Schema annotatedSchema = annotateStringMapInPartialUpdateSchema(schemaEntry.getSchema());
    return new DerivedSchemaEntry(schemaEntry.getValueSchemaID(), schemaEntry.getId(), annotatedSchema);
  }

  public static RmdSchemaEntry getAnnotatedStringMapRmdSchemaEntry(RmdSchemaEntry schemaEntry) {
    if (schemaEntry == null) {
      return null;
    }

    Schema annotatedSchema = annotateStringMapInRmdSchema(schemaEntry.getSchema());
    return new RmdSchemaEntry(schemaEntry.getValueSchemaID(), schemaEntry.getId(), annotatedSchema);
  }

  private static void annotateMapSchema(Schema mapSchema) {
    AvroCompatibilityHelper.setSchemaPropFromJsonString(mapSchema, "avro.java.string", "\"String\"", false);
  }

  private static void annotateStringArraySchema(Schema arraySchema) {
    AvroCompatibilityHelper
        .setSchemaPropFromJsonString(arraySchema.getElementType(), "avro.java.string", "\"String\"", false);
  }
}
