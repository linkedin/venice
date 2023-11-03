package com.linkedin.davinci.schema;

import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_POS;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_FIELD_POS;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import java.util.List;
import org.apache.avro.Schema;


public class SchemaUtils {
  private SchemaUtils() {
    // Utility class
  }

  /**
   * Annotate all the top-level map field and string array field of the input schema to use Java String as key.
   * @param schema the input value schema to be annotated.
   * @return Annotated value schema.
   */
  public static Schema annotateValueSchema(Schema schema) {
    // Create duplicate schema here in order not to create any side effect during annotation.
    Schema replicatedSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schema.toString());
    if (replicatedSchema.getType().equals(RECORD)) {
      for (Schema.Field field: replicatedSchema.getFields()) {
        if (field.schema().isUnion()) {
          for (Schema unionBranchSchema: field.schema().getTypes()) {
            annotateMapAndStringArraySchema(unionBranchSchema);
          }
        } else {
          annotateMapAndStringArraySchema(field.schema());
        }
      }
    }
    return replicatedSchema;
  }

  /**
   * Annotate all the top-level map field and string array of the update schema to use Java String as key.
   * This method will make sure field update and collection merging operations of these fields are annotated.
   * @param schema the input update schema to be annotated.
   * @return Annotated update schema.
   */
  public static Schema annotateUpdateSchema(Schema schema) {
    // Create duplicate schema here in order not to create any side effect during annotation.
    Schema replicatedSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schema.toString());
    if (replicatedSchema.getType().equals(RECORD)) {
      for (Schema.Field field: replicatedSchema.getFields()) {
        if (field.schema().isUnion()) {
          for (Schema unionBranchSchema: field.schema().getTypes()) {
            // Full update request for Map field.
            if (isMap(unionBranchSchema)) {
              annotateMapSchema(unionBranchSchema);
            } else if (isStringArray(unionBranchSchema)) {
              // Full update request for String array field.
              annotateStringArraySchema(unionBranchSchema);
            } else if (unionBranchSchema.getType().equals(RECORD)) {
              for (Schema.Field updateOpField: unionBranchSchema.getFields()) {
                if (isMap(updateOpField.schema())) {
                  annotateMapSchema(updateOpField.schema());
                } else if (isStringArray(updateOpField.schema())) {
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

  /**
   * Annotate all the top-level map and string array's deleted elements field of the RMD schema to use Java String as key.
   * This method will make sure deleted elements field of these fields are annotated.
   * @param schema the input update schema to be annotated.
   * @return Annotated update schema.
   */
  public static Schema annotateRmdSchema(Schema schema) {
    // Create duplicate schema here in order not to create any side effect during annotation.
    Schema replicatedSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schema.toString());
    for (Schema fieldLevelTsSchema: replicatedSchema.getFields().get(TIMESTAMP_FIELD_POS).schema().getTypes()) {
      if (fieldLevelTsSchema.getType().equals(LONG)) {
        continue;
      }
      for (Schema.Field field: fieldLevelTsSchema.getFields()) {
        // For current RMD schema structure, there will be no union, adding this just for defensive coding.
        if (!field.schema().isUnion() && field.schema().getType().equals(RECORD)) {
          Schema.Field deletedElementField = field.schema().getFields().get(DELETED_ELEM_FIELD_POS);
          if (deletedElementField != null && isStringArray(deletedElementField.schema())) {
            annotateStringArraySchema(deletedElementField.schema());
          }
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
  public static SchemaEntry getAnnotatedValueSchemaEntry(SchemaEntry schemaEntry) {
    if (schemaEntry == null) {
      return null;
    }
    Schema annotatedSchema = annotateValueSchema(schemaEntry.getSchema());
    return new SchemaEntry(schemaEntry.getId(), annotatedSchema);
  }

  /**
   * Create a new {@link DerivedSchemaEntry} for partial update schema annotation.
   * @param schemaEntry Input {@link DerivedSchemaEntry}, containing the partial update schema to be annotated.
   * @return Annotated partial update schema in a newly created {@link DerivedSchemaEntry}
   */
  public static DerivedSchemaEntry getAnnotatedDerivedSchemaEntry(DerivedSchemaEntry schemaEntry) {
    if (schemaEntry == null) {
      return null;
    }
    Schema annotatedSchema = annotateUpdateSchema(schemaEntry.getSchema());
    return new DerivedSchemaEntry(schemaEntry.getValueSchemaID(), schemaEntry.getId(), annotatedSchema);
  }

  public static RmdSchemaEntry getAnnotatedRmdSchemaEntry(RmdSchemaEntry schemaEntry) {
    if (schemaEntry == null) {
      return null;
    }

    Schema annotatedSchema = annotateRmdSchema(schemaEntry.getSchema());
    return new RmdSchemaEntry(schemaEntry.getValueSchemaID(), schemaEntry.getId(), annotatedSchema);
  }

  private static void annotateMapSchema(Schema mapSchema) {
    AvroCompatibilityHelper.setSchemaPropFromJsonString(mapSchema, "avro.java.string", "\"String\"", false);
  }

  private static void annotateStringArraySchema(Schema arraySchema) {
    AvroCompatibilityHelper
        .setSchemaPropFromJsonString(arraySchema.getElementType(), "avro.java.string", "\"String\"", false);
  }

  private static boolean isStringArray(Schema schema) {
    return schema.getType().equals(ARRAY) && schema.getElementType().getType().equals(STRING);
  }

  private static boolean isMap(Schema schema) {
    return schema.getType().equals(MAP);
  }

  private static void annotateMapAndStringArraySchema(Schema schema) {
    if (isMap(schema)) {
      annotateMapSchema(schema);
    } else if (isStringArray(schema)) {
      annotateStringArraySchema(schema);
    }
  }

  /**
   * This function returns the type of the field if and only if it has a single non-null type. In cases where the field
   * is a union, it will return the type of the non-null branch as long as there is at most one such branch, otherwise
   * it will return {@link Schema.Type#UNION}.
   */
  public static Schema.Type unwrapOptionalUnion(Schema fieldSchema) {
    switch (fieldSchema.getType()) {
      case UNION:
        List<Schema> unionBranches = fieldSchema.getTypes();
        switch (unionBranches.size()) {
          case 2:
            Schema.Type first = unionBranches.get(0).getType();
            Schema.Type second = unionBranches.get(1).getType();
            if (first == NULL) {
              return second;
            } else if (second == NULL) {
              return first;
            } else {
              return UNION;
            }
          case 1:
            // It's not very clean to have a single branch union, but it could happen...
            return unionBranches.get(0).getType();
          default:
            return UNION;
        }
      default:
        return fieldSchema.getType();
    }
  }

  /**
   * This method checks if the field contains expected type. It relies on {@link SchemaUtils#unwrapOptionalUnion(Schema)}
   * to retrieve the type from the field if incoming field is union.
   * If it could not find expected type, or the incoming union is not valid for processing, it will throw {@link IllegalStateException}
   */
  public static void validateFieldSchemaType(String fieldName, Schema fieldSchema, Schema.Type expectedType) {
    final Schema.Type fieldSchemaType = SchemaUtils.unwrapOptionalUnion(fieldSchema);
    if (fieldSchemaType != expectedType) {
      throw new IllegalStateException(
          String.format("Expect field %s to be of type %s. But got: %s", fieldName, expectedType, fieldSchemaType));
    }
  }
}
