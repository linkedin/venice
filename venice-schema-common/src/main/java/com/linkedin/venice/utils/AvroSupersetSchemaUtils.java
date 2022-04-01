package com.linkedin.venice.utils;

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
   * @param s1 1st input schema
   * @param s2 2nd input schema
   * @return super set schema of s1 and s2
   */
  public static Schema generateSuperSetSchema(Schema s1, Schema s2) {
    if (s1.getType() != s2.getType()) {
      throw new VeniceException("Incompatible schema");
    }
    if (Objects.equals(s1, s2)) {
      return s1;
    }

    // Special handling for String vs Avro string comparison,
    // return the schema with avro.java.string property for string type
    if (s1.getType() == Schema.Type.STRING) {
      return AvroCompatibilityHelper.getSchemaPropAsJsonString(s1, "avro.java.string") == null ? s2 : s1;
    }

    switch (s1.getType()) {
      case RECORD:
        if (!StringUtils.equals(s1.getNamespace(), s2.getNamespace())) {
          throw new VeniceException("Trying to merge schema with different namespace.");
        }
        Schema superSetSchema = Schema.createRecord(s1.getName(), s1.getDoc(), s1.getNamespace(), false);
        superSetSchema.setFields(mergeFieldSchemas(s1, s2));
        return superSetSchema;
      case ARRAY:
        return Schema.createArray(generateSuperSetSchema(s1.getElementType(), s2.getElementType()));
      case MAP:
        return Schema.createMap(generateSuperSetSchema(s1.getValueType(), s2.getValueType()));
      case UNION:
        return unionSchema(s1, s2);
      default:
        throw new VeniceException("Super set schema not supported");
    }
  }

  private static Schema unionSchema(Schema s1, Schema s2) {
    List<Schema> combinedSchema = new ArrayList<>();
    Map<String, Schema> s2Schema = s2.getTypes().stream().collect(Collectors.toMap(s -> s.getName(), s -> s));
    for (Schema subSchemaInS1 : s1.getTypes()) {
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

    for (Schema.Field f1 : s1.getFields()) {
      Schema.Field f2 = s2.getField(f1.name());
      FieldBuilder fieldBuilder = AvroCompatibilityHelper.newField(f1);

      // set default as AvroCompatibilityHelper builder might drop defaults if there is type mismatch
      if (f1.hasDefaultValue()) {
        fieldBuilder.setDefault(AvroCompatibilityHelper.getGenericDefaultValue(f1));
      }
      if (f2 != null) {
        fieldBuilder.setSchema(generateSuperSetSchema(f1.schema(), f2.schema())).setDoc(f1.doc() != null ? f1.doc() : f2.doc());
      }
      fields.add(fieldBuilder.build());
    }

    for (Schema.Field f2 : s2.getFields()) {
      if (s1.getField(f2.name()) == null) {
        FieldBuilder fieldBuilder =  AvroCompatibilityHelper.newField(f2);
        if (f2.hasDefaultValue()) {
          // set default as AvroCompatibilityHelper builder might drop defaults if there is type mismatch
          fieldBuilder.setDefault(AvroCompatibilityHelper.getGenericDefaultValue(f2));
        }
        fields.add(fieldBuilder.build());
      }
    }
    return fields;
  }
}
