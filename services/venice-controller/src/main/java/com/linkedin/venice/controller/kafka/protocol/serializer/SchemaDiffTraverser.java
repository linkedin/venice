package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.Pair;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
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
  public boolean traverse(
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
  private boolean traverseRecord(
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
  private boolean traverseArray(
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
  private boolean traverseMap(
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
  private boolean traverseUnion(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator) {

    String objectName = ((GenericRecord) object).getSchema().getName();
    String fieldPath = buildFieldPath(parentName, objectName);

    for (Schema subCurrentSchema: currentSchema.getTypes()) {
      if (!subCurrentSchema.getName().equals(objectName))
        continue;
      boolean found = false;
      for (Schema subTargetSchema: targetSchema.getTypes()) {
        if (subCurrentSchema.getFullName().equals(subTargetSchema.getFullName())) {
          found = true;
          if (traverse(object, defaultValue, subCurrentSchema, subTargetSchema, fieldPath, validator))
            return true;
          break;
        }
      }

      // If the sub schema is not found in the target schema, we need to apply the validator on this sub schema
      if (!found && validate(validator, object, fieldPath, defaultValue, subCurrentSchema, null))
        return true;
    }
    return false;
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
  private boolean validate(
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator,
      Object object,
      String fieldName,
      Object fieldDefaultValue,
      Schema currentSchema,
      Schema targetSchema) {
    return validator.apply(
        object,
        new Pair<>(
            generateField(fieldName, currentSchema, fieldDefaultValue),
            targetSchema == null ? null : generateField(fieldName, targetSchema, fieldDefaultValue)));
  }

  /**
   * Check if the schema is a nested type
   * @param schema the schema to be checked
   * @return true if the schema is a nested type, false otherwise
   */
  private boolean isNestedType(Schema schema) {
    return EnumSet.of(Schema.Type.RECORD, Schema.Type.UNION, Schema.Type.ARRAY, Schema.Type.MAP)
        .contains(schema.getType());
  }

  /**
   * Generate a field with the given name and schema
   * @param name the name of the field
   * @param schema the schema of the field
   * @return the generated field
   */
  private static Schema.Field generateField(String name, Schema schema, Object defaultValue) {
    return AvroCompatibilityHelper.createSchemaField(name, schema, "", defaultValue);
  }

  private static String buildFieldPath(String parent, String field) {
    return parent.isEmpty() ? field : parent + "_" + field;
  }

  /**
   * Create a semantic check function to validate the object.
   * The object is valid if it satisfies the following conditions:
   * <ol>
   *   <li>The object is null</li>
   *   <li>The object is a nested type, and the object equals to the default value of that object</li>
   *   <li>The object is a non nested field, and the value equals to default value from schema or the default value of the schema type</li>
   * </ol>
   * @param errorMessage: the error message to be set if the object is invalid
   */
  public BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> createSemanticCheck(
      AtomicReference<String> errorMessage) {
    return (object, schemasPair) -> {
      Schema.Field currentField = schemasPair.getFirst();
      Schema.Field targetField = schemasPair.getSecond();

      if (currentField == null || object == null) {
        return false;
      }

      if (targetField == null) {
        return handleMissingTargetField(object, currentField, errorMessage);
      }

      if (hasSchemaTypeMismatch(currentField, targetField, errorMessage)) {
        return true;
      }

      if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(currentField.schema(), targetField.schema())) {
        return false;
      }

      return checkSchemaSpecificCases(object, currentField, targetField, errorMessage);
    };
  }

  private boolean handleMissingTargetField(
      Object object,
      Schema.Field currentField,
      AtomicReference<String> errorMessage) {
    if (isNestedType(currentField.schema())) {
      Object fieldDefaultValue = AvroSchemaUtils.getFieldDefault(currentField);
      if (fieldDefaultValue == null) {
        errorMessage.set(
            "Field: " + formatFieldName(currentField)
                + " is not in the target schema, and object is non-null. Actual value: " + object);
        return true;
      }
      if (!fieldDefaultValue.equals(object)) {
        errorMessage.set(
            "Field: " + formatFieldName(currentField) + " contains non-default value. Actual value: " + object
                + ". Default value: [" + fieldDefaultValue + ", " + null + "]");
        return true;
      }
      return false;
    }

    return isNonDefaultValue(object, currentField, errorMessage);
  }

  private boolean hasSchemaTypeMismatch(
      Schema.Field currentField,
      Schema.Field targetField,
      AtomicReference<String> errorMessage) {
    if (currentField.schema().getType() != targetField.schema().getType()) {
      errorMessage.set("Field: " + formatFieldName(currentField) + " has different types");
      return true;
    }
    return false;
  }

  private boolean checkSchemaSpecificCases(
      Object object,
      Schema.Field currentField,
      Schema.Field targetField,
      AtomicReference<String> errorMessage) {
    switch (currentField.schema().getType()) {
      case ENUM:
        return isNewEnumValue(object, currentField, targetField, errorMessage);
      case FIXED:
        if (currentField.schema().getFixedSize() != targetField.schema().getFixedSize()) {
          errorMessage.set("Field: " + formatFieldName(currentField) + " has different fixed size");
          return true;
        }
        return false;
      default:
        return isNonDefaultValue(object, currentField, errorMessage);
    }
  }

  private String formatFieldName(Schema.Field field) {
    return field.name().replace("_", ".");
  }

  private boolean isNewEnumValue(
      Object object,
      Schema.Field currentField,
      Schema.Field targetField,
      AtomicReference<String> errorMessage) {
    List<String> currentSymbols = currentField.schema().getEnumSymbols();
    List<String> targetSymbols = targetField.schema().getEnumSymbols();
    if (targetSymbols.stream().anyMatch(symbol -> !currentSymbols.contains(symbol) && symbol.equals(object))) {
      errorMessage.set("Field: " + currentField.name().replace("_", ".") + " contains new enum value: " + object);
      return true;
    }

    return false;
  }

  private boolean isNonDefaultValue(Object value, Schema.Field field, AtomicReference<String> errorMessage) {
    if (value == null)
      return false;

    Map<Schema.Type, Object> defaultValues = new HashMap<>();
    defaultValues.put(Schema.Type.STRING, "");
    defaultValues.put(Schema.Type.BOOLEAN, false);
    defaultValues.put(Schema.Type.INT, 0);
    defaultValues.put(Schema.Type.LONG, 0L);
    defaultValues.put(Schema.Type.FLOAT, 0.0f);
    defaultValues.put(Schema.Type.DOUBLE, 0.0d);
    defaultValues.put(Schema.Type.BYTES, new byte[0]);

    Object fieldDefaultValue = AvroSchemaUtils.getFieldDefault(field);
    Object defaultValue = defaultValues.getOrDefault(field.schema().getType(), null);

    if (!value.equals(defaultValue) && !value.equals(fieldDefaultValue)) {
      errorMessage.set(
          "Field: " + field.name().replace("_", ".") + " contains non-default value. Actual value: " + value
              + ". Default value: [" + fieldDefaultValue + ", " + defaultValue + "]");
      return true;
    }
    return false;
  }
}
