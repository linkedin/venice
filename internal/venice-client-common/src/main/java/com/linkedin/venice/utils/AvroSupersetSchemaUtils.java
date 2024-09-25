package com.linkedin.venice.utils;

import static com.linkedin.venice.utils.AvroSchemaUtils.getFieldDefault;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;


public class AvroSupersetSchemaUtils {
  private AvroSupersetSchemaUtils() {
    // Utility class.
  }

  /**
   * @return True if {@param s1} is {@param s2}'s superset schema and false otherwise.
   */
  public static boolean isSupersetSchema(Schema s1, Schema s2) {
    final Schema supersetSchema = generateSuperSetSchema(s1, s2);
    return AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, supersetSchema);
  }

  /**
   * Generate super-set schema of two Schemas. If we have {A,B,C} and {A,B,D} it will generate {A,B,C,D}, where
   * C/D could be nested record change as well eg, array/map of records, or record of records.
   * Prerequisite: The top-level schema are of type RECORD only and each field have default values. ie they are compatible
   * schemas and the generated schema will pick the default value from new value schema.
   * @param existingSchema schema existing in the repo
   * @param newSchema schema to be added.
   * @return super-set schema of existingSchema abd newSchema
   */
  public static Schema generateSuperSetSchema(Schema existingSchema, Schema newSchema) {
    if (existingSchema.getType() != newSchema.getType()) {
      throw new VeniceException("Incompatible schema");
    }
    if (Objects.equals(existingSchema, newSchema)) {
      return existingSchema;
    }

    // Special handling for String vs Avro string comparison,
    // return the schema with avro.java.string property for string type
    if (existingSchema.getType() == Schema.Type.STRING) {
      return AvroCompatibilityHelper.getSchemaPropAsJsonString(existingSchema, "avro.java.string") == null
          ? newSchema
          : existingSchema;
    }

    switch (existingSchema.getType()) {
      case RECORD:
        if (!StringUtils.equals(existingSchema.getNamespace(), newSchema.getNamespace())) {
          throw new VeniceException(
              String.format(
                  "Trying to merge record schemas with different namespace. "
                      + "Got existing schema namespace: %s and new schema namespace: %s",
                  existingSchema.getNamespace(),
                  newSchema.getNamespace()));
        }
        if (!StringUtils.equals(existingSchema.getName(), newSchema.getName())) {
          throw new VeniceException(
              String.format(
                  "Trying to merge record schemas with different name. "
                      + "Got existing schema name: %s and new schema name: %s",
                  existingSchema.getName(),
                  newSchema.getName()));
        }

        Schema superSetSchema = Schema
            .createRecord(existingSchema.getName(), existingSchema.getDoc(), existingSchema.getNamespace(), false);
        superSetSchema.setFields(mergeFieldSchemas(newSchema, existingSchema));
        return superSetSchema;
      case ARRAY:
        return Schema.createArray(generateSuperSetSchema(existingSchema.getElementType(), newSchema.getElementType()));
      case MAP:
        return Schema.createMap(generateSuperSetSchema(existingSchema.getValueType(), newSchema.getValueType()));
      case UNION:
        return unionSchema(newSchema, existingSchema);
      default:
        throw new VeniceException("Super set schema not supported");
    }
  }

  /**
   * Merge union schema from two schema object. The rule is: If a field exist in both new schema and old schema, we should
   * generate the superset schema of these two versions of the same field, with new schema's information taking higher
   * priority.
   * @param s1 new union schema
   * @param s2 old union schema
   * @return merged schema field
   */
  private static Schema unionSchema(Schema s1, Schema s2) {
    List<Schema> combinedSchema = new ArrayList<>();
    Map<String, Schema> s2Schema = s2.getTypes().stream().collect(Collectors.toMap(Schema::getName, s -> s));
    for (Schema subSchemaInS1: s1.getTypes()) {
      final String fieldName = subSchemaInS1.getName();
      final Schema subSchemaInS2 = s2Schema.get(fieldName);
      if (subSchemaInS2 == null) {
        combinedSchema.add(subSchemaInS1);
      } else {
        combinedSchema.add(generateSuperSetSchema(subSchemaInS2, subSchemaInS1));
        s2Schema.remove(fieldName);
      }
    }
    s2Schema.forEach((k, v) -> combinedSchema.add(v));
    return Schema.createUnion(combinedSchema);
  }

  private static void copyFieldProperties(FieldBuilder fieldBuilder, Schema.Field field) {
    AvroCompatibilityHelper.getAllPropNames(field).forEach(k -> {
      String propValue = AvroCompatibilityHelper.getFieldPropAsJsonString(field, k);
      if (propValue != null) {
        fieldBuilder.addProp(k, propValue);
      }
    });
  }

  private static FieldBuilder deepCopySchemaFieldWithoutFieldProps(Schema.Field field) {
    FieldBuilder fieldBuilder = AvroCompatibilityHelper.newField(null)
        .setName(field.name())
        .setSchema(field.schema())
        .setDoc(field.doc())
        .setOrder(field.order());
    // set default as AvroCompatibilityHelper builder might drop defaults if there is type mismatch
    if (field.hasDefaultValue()) {
      fieldBuilder.setDefault(getFieldDefault(field));
    }
    return fieldBuilder;
  }

  private static FieldBuilder deepCopySchemaField(Schema.Field field) {
    FieldBuilder fieldBuilder = deepCopySchemaFieldWithoutFieldProps(field);
    copyFieldProperties(fieldBuilder, field);
    return fieldBuilder;
  }

  /**
   * Merge field schema from two schema object. The rule is: If a field exist in both new schema and old schema, we should
   * generate the superset schema of these two versions of the same field, with new schema's information taking higher
   * priority.
   * @param s1 new schema
   * @param s2 old schema
   * @return merged schema field
   */
  private static List<Schema.Field> mergeFieldSchemas(Schema s1, Schema s2) {
    List<Schema.Field> fields = new ArrayList<>();

    for (Schema.Field f1: s1.getFields()) {
      Schema.Field f2 = s2.getField(f1.name());

      FieldBuilder fieldBuilder = deepCopySchemaField(f1);
      if (f2 != null) {
        fieldBuilder.setSchema(generateSuperSetSchema(f2.schema(), f1.schema()))
            .setDoc(f1.doc() != null ? f1.doc() : f2.doc());
      }
      fields.add(fieldBuilder.build());
    }

    for (Schema.Field f2: s2.getFields()) {
      if (s1.getField(f2.name()) == null) {
        fields.add(deepCopySchemaField(f2).build());
      }
    }
    return fields;
  }

  public static MultiSchemaResponse.Schema getSupersetSchemaFromSchemaResponse(
      MultiSchemaResponse schemaResponse,
      int supersetSchemaId) {
    for (MultiSchemaResponse.Schema schema: schemaResponse.getSchemas()) {
      if (schema.getId() != supersetSchemaId) {
        continue;
      }
      if (schema.getDerivedSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
        continue;
      }
      if (schema.getRmdValueSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
        continue;
      }
      return schema;
    }
    return null;
  }

  public static MultiSchemaResponse.Schema getLatestUpdateSchemaFromSchemaResponse(
      MultiSchemaResponse schemaResponse,
      int supersetSchemaId) {
    MultiSchemaResponse.Schema updateSchema = null;
    for (MultiSchemaResponse.Schema schema: schemaResponse.getSchemas()) {
      if (schema.getId() != supersetSchemaId) {
        continue;
      }
      if (schema.getDerivedSchemaId() == SchemaData.INVALID_VALUE_SCHEMA_ID) {
        continue;
      }
      if (updateSchema == null || schema.getDerivedSchemaId() > updateSchema.getDerivedSchemaId()) {
        updateSchema = schema;
      }
    }
    return updateSchema;
  }

  /**
   * * Validate if the Subset Value Schema is a subset of the Superset Value Schema, here the field props are not used to
   * check if the field is same or not.
   */
  public static boolean validateSubsetValueSchema(Schema subsetValueSchema, String supersetSchemaStr) {
    Schema supersetSchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(supersetSchemaStr);
    for (Schema.Field field: subsetValueSchema.getFields()) {
      Schema.Field fieldInSupersetSchema = supersetSchema.getField(field.name());
      if (fieldInSupersetSchema == null) {
        return false;
      }
      Schema.Field subsetValueSchemaWithoutFieldProps = deepCopySchemaFieldWithoutFieldProps(field).build();
      Schema.Field fieldInSupersetSchemaWithoutFieldProps =
          deepCopySchemaFieldWithoutFieldProps(fieldInSupersetSchema).build();
      if (!subsetValueSchemaWithoutFieldProps.equals(fieldInSupersetSchemaWithoutFieldProps)) {
        return false;
      }
    }
    return true;
  }
}
