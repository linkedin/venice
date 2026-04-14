package com.linkedin.venice.utils;

import static com.linkedin.venice.utils.AvroSchemaUtils.getFieldDefault;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaData;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
    final Schema supersetSchema = generateSupersetSchema(s1, s2);
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
  public static Schema generateSupersetSchema(Schema existingSchema, Schema newSchema) {
    // Unwrap single-element unions to their inner type so that [T] is treated
    // equivalently to T during type comparison and superset generation.
    final Schema unwrappedExistingSchema = unwrapSingleElementUnion(existingSchema);
    final Schema unwrappedNewSchema = unwrapSingleElementUnion(newSchema);

    if (unwrappedExistingSchema.getType() != unwrappedNewSchema.getType()) {
      throw new VeniceException("Incompatible schema");
    }
    if (Objects.equals(unwrappedExistingSchema, unwrappedNewSchema)) {
      return unwrappedExistingSchema;
    }

    // Special handling for String vs Avro string comparison,
    // return the schema with avro.java.string property for string type
    if (unwrappedExistingSchema.getType() == Schema.Type.STRING) {
      return AvroCompatibilityHelper.getSchemaPropAsJsonString(unwrappedExistingSchema, "avro.java.string") == null
          ? unwrappedNewSchema
          : unwrappedExistingSchema;
    }

    switch (unwrappedExistingSchema.getType()) {
      case RECORD:
        if (!StringUtils.equals(unwrappedExistingSchema.getNamespace(), unwrappedNewSchema.getNamespace())) {
          throw new VeniceException(
              String.format(
                  "Trying to merge record schemas with different namespace. "
                      + "Got existing schema namespace: %s and new schema namespace: %s",
                  unwrappedExistingSchema.getNamespace(),
                  unwrappedNewSchema.getNamespace()));
        }
        if (!StringUtils.equals(unwrappedExistingSchema.getName(), unwrappedNewSchema.getName())) {
          throw new VeniceException(
              String.format(
                  "Trying to merge record schemas with different name. "
                      + "Got existing schema name: %s and new schema name: %s",
                  unwrappedExistingSchema.getName(),
                  unwrappedNewSchema.getName()));
        }

        Schema superSetSchema = Schema.createRecord(
            unwrappedExistingSchema.getName(),
            unwrappedExistingSchema.getDoc(),
            unwrappedExistingSchema.getNamespace(),
            false);
        superSetSchema.setFields(mergeFieldSchemas(unwrappedExistingSchema, unwrappedNewSchema));
        return superSetSchema;
      case ARRAY:
        return Schema.createArray(
            generateSupersetSchema(unwrappedExistingSchema.getElementType(), unwrappedNewSchema.getElementType()));
      case MAP:
        return Schema.createMap(
            generateSupersetSchema(unwrappedExistingSchema.getValueType(), unwrappedNewSchema.getValueType()));
      case UNION:
        return unionSchema(unwrappedExistingSchema, unwrappedNewSchema);
      case ENUM: {
        // Build a superset symbol list: all symbols from existingSchema (preserving their order),
        // followed by any symbols present only in newSchema. This ensures no symbol is lost when
        // the two schemas have diverged (e.g. existing has ["A","B","C"], new has ["A","B","D"]).
        LinkedHashSet<String> supersetSymbols = new LinkedHashSet<>(unwrappedExistingSchema.getEnumSymbols());
        supersetSymbols.addAll(unwrappedNewSchema.getEnumSymbols());
        // Always construct a new enum schema so that properties from both schemas are merged
        // consistently, regardless of whether symbols grew or diverged. (Truly-equal schemas are
        // already short-circuited by the Objects.equals check at the top of this method.)
        // newSchema takes priority on conflicts. Avro 1.10+ forbids overwriting an already-set
        // property, so each prop is written exactly once: existingSchema-only props first, then
        // all newSchema props.
        Schema supersetEnum = Schema.createEnum(
            unwrappedNewSchema.getName(),
            unwrappedNewSchema.getDoc(),
            unwrappedNewSchema.getNamespace(),
            new ArrayList<>(supersetSymbols),
            unwrappedNewSchema.getEnumDefault());
        Set<String> newSchemaPropNames = getSchemaPropNames(unwrappedNewSchema);
        getSchemaPropNames(unwrappedExistingSchema).stream()
            .filter(prop -> !newSchemaPropNames.contains(prop))
            .forEach(
                prop -> AvroCompatibilityHelper.setSchemaPropFromJsonString(
                    supersetEnum,
                    prop,
                    AvroCompatibilityHelper.getSchemaPropAsJsonString(unwrappedExistingSchema, prop),
                    false));
        getSchemaPropNames(unwrappedNewSchema).forEach(
            prop -> AvroCompatibilityHelper.setSchemaPropFromJsonString(
                supersetEnum,
                prop,
                AvroCompatibilityHelper.getSchemaPropAsJsonString(unwrappedNewSchema, prop),
                false));
        return supersetEnum;
      }
      case FIXED: {
        // FIXED schemas are structurally compatible only when their size attributes are identical.
        // A size mismatch is an irreconcilable schema incompatibility — unlike custom properties,
        // the size is part of the binary encoding and cannot be silently promoted.
        if (unwrappedExistingSchema.getFixedSize() != unwrappedNewSchema.getFixedSize()) {
          throw new VeniceException(
              String.format(
                  "Incompatible FIXED schemas for '%s': existing size %d does not match new size %d",
                  unwrappedExistingSchema.getFullName(),
                  unwrappedExistingSchema.getFixedSize(),
                  unwrappedNewSchema.getFixedSize()));
        }
        // Sizes match — merge properties from both schemas, same convention as ENUM:
        // existingSchema-only props first, then all newSchema props (newSchema wins on conflicts).
        Schema supersetFixed = Schema.createFixed(
            unwrappedNewSchema.getName(),
            unwrappedNewSchema.getDoc(),
            unwrappedNewSchema.getNamespace(),
            unwrappedNewSchema.getFixedSize());
        Set<String> newFixedPropNames = getSchemaPropNames(unwrappedNewSchema);
        getSchemaPropNames(unwrappedExistingSchema).stream()
            .filter(prop -> !newFixedPropNames.contains(prop))
            .forEach(
                prop -> AvroCompatibilityHelper.setSchemaPropFromJsonString(
                    supersetFixed,
                    prop,
                    AvroCompatibilityHelper.getSchemaPropAsJsonString(unwrappedExistingSchema, prop),
                    false));
        getSchemaPropNames(unwrappedNewSchema).forEach(
            prop -> AvroCompatibilityHelper.setSchemaPropFromJsonString(
                supersetFixed,
                prop,
                AvroCompatibilityHelper.getSchemaPropAsJsonString(unwrappedNewSchema, prop),
                false));
        return supersetFixed;
      }
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case NULL:
        // Primitive types cannot differ structurally; schemas are equal in type but differ only in
        // custom properties (e.g. "li.data.proto.numberFieldType"). Return unwrappedNewSchema so
        // its properties take priority, consistent with the convention used elsewhere in this method.
        return unwrappedNewSchema;
      default:
        throw new VeniceException("Super set schema not supported");
    }
  }

  /**
   * Merge union schema from two schema object. The rule is: If a field exist in both new schema and old schema, we should
   * generate the superset schema of these two versions of the same field, with new schema's information taking higher
   * priority.
   */
  private static Schema unionSchema(Schema existingSchema, Schema newSchema) {
    List<Schema> combinedSchema = new ArrayList<>();
    Map<String, Schema> existingSchemaTypeMap =
        existingSchema.getTypes().stream().collect(Collectors.toMap(Schema::getName, s -> s));
    for (Schema subSchemaInNewSchema: newSchema.getTypes()) {
      final String fieldName = subSchemaInNewSchema.getName();
      final Schema subSchemaInExistingSchema = existingSchemaTypeMap.get(fieldName);
      if (subSchemaInExistingSchema == null) {
        combinedSchema.add(subSchemaInNewSchema);
      } else {
        combinedSchema.add(generateSupersetSchema(subSchemaInExistingSchema, subSchemaInNewSchema));
        existingSchemaTypeMap.remove(fieldName);
      }
    }
    existingSchemaTypeMap.forEach((k, v) -> combinedSchema.add(v));
    return Schema.createUnion(combinedSchema);
  }

  /**
   * If the schema is a UNION with exactly one member type, return that inner type.
   * A single-element union {@code [T]} is semantically equivalent to {@code T} in Avro,
   * but {@link Schema#getType()} returns {@link Schema.Type#UNION} for the wrapper form.
   * Unwrapping normalizes both representations so they compare as the same type.
   */
  private static Schema unwrapSingleElementUnion(Schema schema) {
    if (schema.getType() == Schema.Type.UNION && schema.getTypes().size() == 1) {
      return schema.getTypes().get(0);
    }
    return schema;
  }

  /**
   * Returns all custom property names for a {@link Schema} using {@link Schema#getObjectProps()} (available since
   * Avro 1.8) instead of {@link AvroCompatibilityHelper#getAllPropNames(Schema)}.  The latter routes through
   * {@code Avro16Adapter.getAllPropNames} which calls the removed {@code Schema.getProps()} and breaks on Avro 1.11+.
   */
  private static Set<String> getSchemaPropNames(Schema schema) {
    return schema.getObjectProps().keySet();
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
   * Merge field schema from two schema object.
   * The rule is: If a field exist in both new schema and old schema, we should generate the superset schema of these
   * two versions of the same field, with new schema's information taking higher priority.
   * For default value, if new schema does not have default value, we will still preserve the old default value.
   * @param newSchema new schema
   * @param existingSchema old schema
   * @return merged schema field
   */
  private static List<Schema.Field> mergeFieldSchemas(Schema existingSchema, Schema newSchema) {
    List<Schema.Field> fields = new ArrayList<>();

    for (Schema.Field fieldInNewSchema: newSchema.getFields()) {
      Schema.Field fieldInExistingSchema = existingSchema.getField(fieldInNewSchema.name());

      FieldBuilder fieldBuilder = deepCopySchemaField(fieldInNewSchema);
      if (fieldInExistingSchema != null) {
        fieldBuilder.setSchema(generateSupersetSchema(fieldInExistingSchema.schema(), fieldInNewSchema.schema()))
            .setDoc(fieldInNewSchema.doc() != null ? fieldInNewSchema.doc() : fieldInExistingSchema.doc());
        if (!fieldInNewSchema.hasDefaultValue() && fieldInExistingSchema.hasDefaultValue()) {
          fieldBuilder.setDefault(getFieldDefault(fieldInExistingSchema));
        }
      }
      Schema.Field generatedField = fieldBuilder.build();
      fields.add(generatedField);
    }

    for (Schema.Field fieldInExistingSchema: existingSchema.getFields()) {
      if (newSchema.getField(fieldInExistingSchema.name()) == null) {
        fields.add(deepCopySchemaField(fieldInExistingSchema).build());
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
