package com.linkedin.venice.utils;

import static com.linkedin.venice.utils.AvroSchemaUtils.getFieldDefault;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.venice.exceptions.VeniceException;
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
   * schemas and the generated schema will pick the default value from s1.
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
        superSetSchema.setFields(mergeFieldSchemas(existingSchema, newSchema));
        return superSetSchema;
      case ARRAY:
        return Schema.createArray(generateSuperSetSchema(existingSchema.getElementType(), newSchema.getElementType()));
      case MAP:
        return Schema.createMap(generateSuperSetSchema(existingSchema.getValueType(), newSchema.getValueType()));
      case UNION:
        return unionSchema(existingSchema, newSchema);
      default:
        throw new VeniceException("Super set schema not supported");
    }
  }

  private static Schema unionSchema(Schema s1, Schema s2) {
    List<Schema> combinedSchema = new ArrayList<>();
    Map<String, Schema> s2Schema = s2.getTypes().stream().collect(Collectors.toMap(s -> s.getName(), s -> s));
    for (Schema subSchemaInS1: s1.getTypes()) {
      final String fieldName = subSchemaInS1.getName();
      final Schema subSchemaWithSameNameInS2 = s2Schema.get(fieldName);
      if (subSchemaWithSameNameInS2 == null) {
        combinedSchema.add(subSchemaInS1);
      } else {
        combinedSchema.add(generateSuperSetSchema(subSchemaInS1, subSchemaWithSameNameInS2));
        s2Schema.remove(fieldName);
      }
    }
    s2Schema.forEach((k, v) -> combinedSchema.add(v));

    return Schema.createUnion(combinedSchema);
  }

  private static List<Schema.Field> mergeFieldSchemas(Schema s1, Schema s2) {
    List<Schema.Field> fields = new ArrayList<>();

    for (Schema.Field f1: s1.getFields()) {
      Schema.Field f2 = s2.getField(f1.name());
      FieldBuilder fieldBuilder = AvroCompatibilityHelper.newField(f1);

      // set default as AvroCompatibilityHelper builder might drop defaults if there is type mismatch
      if (f1.hasDefaultValue()) {
        fieldBuilder.setDefault(getFieldDefault(f1));
      }
      if (f2 != null) {
        fieldBuilder.setSchema(generateSuperSetSchema(f1.schema(), f2.schema()))
            .setDoc(f1.doc() != null ? f1.doc() : f2.doc());
      }
      fields.add(fieldBuilder.build());
    }

    for (Schema.Field f2: s2.getFields()) {
      if (s1.getField(f2.name()) == null) {
        FieldBuilder fieldBuilder = AvroCompatibilityHelper.newField(f2);
        if (f2.hasDefaultValue()) {
          // set default as AvroCompatibilityHelper builder might drop defaults if there is type mismatch
          fieldBuilder.setDefault(getFieldDefault(f2));
        }
        fields.add(fieldBuilder.build());
      }
    }
    return fields;
  }
}
