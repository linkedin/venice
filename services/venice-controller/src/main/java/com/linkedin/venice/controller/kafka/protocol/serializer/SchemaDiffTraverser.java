package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.Pair;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Traverses an object using its current schema and compares each sub-schema (node/field)
 * to the corresponding field in the target schema.
 * If a difference is found, a validator function is applied to determine validity.
 * The validator returns {@code true} if the object is invalid, otherwise {@code false}.
 * The traversal stops immediately if an invalid object is detected.
 */
public class SchemaDiffTraverser {
  /**
   * Traverse the object with the current schema and compare it with the target schema.
   * @param object the object to be traversed
   * @param defaultValue the default value of the object derived from the schema
   * @param currentSchema the current schema of the object
   * @param targetSchema the target schema to be compared with
   * @param parentName the parent name of the object
   * @param validator the validator function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  public static boolean traverse(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator) {

    // If the object is null, we don't need to traverse it
    if (object == null)
      return false;

    // If the current schema is null, we a
    if (targetSchema == null)
      return validate(validator, object, parentName, defaultValue, currentSchema, null);

    // If the current schema type is different from the target schema type, we need to apply the validator
    if (currentSchema.getType() != targetSchema.getType())
      return validate(validator, object, parentName, defaultValue, currentSchema, targetSchema);

    // If the current schema is the same as the target schema, we don't need to traverse it
    if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(currentSchema, targetSchema))
      return false;

    // Traverse the object based on the schema type
    switch (currentSchema.getType()) {
      case RECORD:
        return traverseRecord(object, defaultValue, currentSchema, targetSchema, parentName, validator);
      case UNION:
        return traverseUnion(object, defaultValue, currentSchema, targetSchema, parentName, validator);
      case ARRAY:
        return traverseArray(object, defaultValue, currentSchema, targetSchema, parentName, validator);
      case MAP:
        return traverseMap(object, defaultValue, currentSchema, targetSchema, parentName, validator);
      default:
        return validate(validator, object, parentName, defaultValue, currentSchema, targetSchema);
    }
  }

  /**
   * Traverse the record object
   * @param object the object to be traversed
   * @param defaultValue the default value of the object derived from the schema
   * @param currentSchema the current schema of the object
   * @param targetSchema the target schema to be compared with
   * @param parentName the parent name of the object
   * @param validator the validator function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  private static boolean traverseRecord(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator) {

    if (targetSchema == null)
      return validate(validator, object, parentName, defaultValue, currentSchema, null);

    for (Schema.Field field: currentSchema.getFields()) {
      String fieldName = buildFieldPath(parentName, field.name());
      Schema.Field targetField = targetSchema.getField(field.name());
      Object value = ((GenericRecord) object).get(field.name());
      Object fieldDefaultValue = AvroSchemaUtils.getFieldDefault(field);
      Schema fieldSchema = field.schema();

      if (targetField == null) {
        if (validate(validator, value, fieldName, fieldDefaultValue, fieldSchema, null))
          return true;
      } else if (isNestedType(field.schema())) {
        if (traverse(value, fieldDefaultValue, fieldSchema, targetField.schema(), fieldName, validator))
          return true;
      } else {
        if (validate(validator, value, fieldName, fieldDefaultValue, fieldSchema, targetField.schema()))
          return true;
      }
    }
    return false;
  }

  /**
   * Traverse the array object
   * @param object the object to be traversed
   * @param defaultValue the default value of the object derived from the schema
   * @param currentSchema the current schema of the object
   * @param targetSchema the target schema to be compared with
   * @param parentName the parent name of the object
   * @param validator the validator function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  private static boolean traverseArray(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator) {

    String arrayName = buildFieldPath(parentName, currentSchema.getName());
    if (isNestedType(currentSchema.getElementType())) {
      String nestedSchemaName = buildFieldPath(arrayName, currentSchema.getElementType().getName());
      List<Object> array = (List<Object>) object;
      for (int i = 0; i < array.size(); i++) {
        if (traverse(
            array.get(i),
            defaultValue,
            currentSchema.getElementType(),
            targetSchema.getElementType(),
            buildFieldPath(nestedSchemaName, String.valueOf(i)),
            validator))
          return true;
      }
    }
    return validate(
        validator,
        object,
        arrayName,
        defaultValue,
        currentSchema.getElementType(),
        targetSchema.getElementType());
  }

  /**
   * Traverse the map object
   * @param object the object to be traversed
   * @param defaultValue the default value of the object derived from the schema
   * @param currentSchema the current schema of the object
   * @param targetSchema the target schema to be compared with
   * @param parentName the parent name of the object
   * @param validator the validator function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  private static boolean traverseMap(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator) {
    String mapName = buildFieldPath(parentName, currentSchema.getName());
    if (isNestedType(currentSchema.getValueType())) {
      Map<String, Object> map = (Map<String, Object>) object;
      String nestedMapName = buildFieldPath(mapName, currentSchema.getValueType().getName());
      for (Map.Entry<String, Object> entry: map.entrySet()) {
        if (traverse(
            entry.getValue(),
            defaultValue,
            currentSchema.getValueType(),
            targetSchema.getValueType(),
            buildFieldPath(nestedMapName, entry.getKey()),
            validator))
          return true;
      }
      return false;
    }
    return validate(
        validator,
        object,
        mapName,
        defaultValue,
        currentSchema.getValueType(),
        targetSchema.getValueType());
  }

  /**
   * Traverse the union object
   * @param object the object to be traversed
   * @param defaultValue the default value of the object derived from the schema
   * @param currentSchema the current schema of the object
   * @param targetSchema the target schema to be compared with
   * @param parentName the parent name of the object
   * @param validator the validator function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  private static boolean traverseUnion(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator) {

    if (currentSchema.getType() != Schema.Type.UNION || targetSchema.getType() != Schema.Type.UNION) {
      throw new IllegalArgumentException(
          "The schema is not a union schema. Current schema type: " + currentSchema.getType() + ". Target schema type: "
              + targetSchema.getType());
    }

    String objectName = ((GenericRecord) object).getSchema().getName();
    String fieldPath = buildFieldPath(parentName, objectName);

    Map<String, Schema> currentSchemaMap =
        currentSchema.getTypes().stream().collect(Collectors.toMap(s -> s.getName(), s -> s));
    Map<String, Schema> targetSchemaMap =
        targetSchema.getTypes().stream().collect(Collectors.toMap(s -> s.getName(), s -> s));

    if (!currentSchemaMap.containsKey(objectName)) {
      throw new IllegalArgumentException(
          "The object schema is not found in the union schema. Available schemas: " + currentSchemaMap.keySet()
              + ". Object name: " + objectName);
    }

    Schema subCurrentSchema = currentSchemaMap.get(objectName);
    Schema subTargetSchema = targetSchemaMap.get(objectName);

    if (subTargetSchema == null) {
      return validate(validator, object, fieldPath, defaultValue, subCurrentSchema, null);
    } else {
      return traverse(object, defaultValue, subCurrentSchema, subTargetSchema, fieldPath, validator);
    }
  }

  /**
   * Apply the validator function to the object
   * @param validator the validator function to validate the object
   * @param object the object to be validated
   * @param fieldName the field name of the object
   * @param fieldDefaultValue the default value of the field derived from the schema
   * @param currentSchema the current schema of the object
   * @param targetSchema the target schema to be compared with
   * @return true if the object is invalid, false otherwise
   */
  private static boolean validate(
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator,
      Object object,
      String fieldName,
      Object fieldDefaultValue,
      Schema currentSchema,
      Schema targetSchema) {
    return validator.apply(
        object,
        new Pair<>(
            AvroCompatibilityHelper.createSchemaField(fieldName, currentSchema, "", fieldDefaultValue),
            targetSchema == null
                ? null
                : AvroCompatibilityHelper.createSchemaField(fieldName, targetSchema, "", fieldDefaultValue)));
  }

  /**
   * Check if the schema is a nested type
   * @param schema the schema to be checked
   * @return true if the schema is a nested type, false otherwise
   */
  private static boolean isNestedType(Schema schema) {
    return EnumSet.of(Schema.Type.RECORD, Schema.Type.UNION, Schema.Type.ARRAY, Schema.Type.MAP)
        .contains(schema.getType());
  }

  private static String buildFieldPath(String parent, String field) {
    return parent.isEmpty() ? field : parent + "_" + field;
  }
}
