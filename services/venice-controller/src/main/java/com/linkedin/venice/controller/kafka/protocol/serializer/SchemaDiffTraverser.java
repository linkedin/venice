package com.linkedin.venice.controller.kafka.protocol.serializer;

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
 * SchemaDiffTraverser class will traverse the object (with current schema) and compare each sub-schema (node) to the corresponding
 * field/schema/node in the target schema.
 * When we detect the difference between the current schema and the target schema, we will apply the filter function to the object.
 * The filter function will contain necessary logic to validate/invalidate the object.
 * The filter function will return true if the object is invalid, and false otherwise.
 * The traverse method return true if the object is invalid (based on filter) and stop the search immediately.
 */
public class SchemaDiffTraverser {
  /**
   * Traverse the object with the current schema and compare it with the target schema.
   * @param object the object to be traversed
   * @param defaultValue the default value of the object derived from the schema
   * @param currentSchema the current schema of the object
   * @param targetSchema the target schema to be compared with
   * @param parentName the parent name of the object
   * @param filter the filter function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  public boolean traverse(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter) {

    // If the object is null, we don't need to traverse it
    if (object == null)
      return false;

    // If the current schema is null, we a
    if (targetSchema == null)
      return applyFilter(filter, object, parentName, defaultValue, currentSchema, null);

    // If the current schema type is different from the target schema type, we need to apply the filter
    if (currentSchema.getType() != targetSchema.getType())
      return applyFilter(filter, object, parentName, defaultValue, currentSchema, targetSchema);

    // If the current schema is the same as the target schema, we don't need to traverse it
    if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(currentSchema, targetSchema))
      return false;

    // Traverse the object based on the schema type
    switch (currentSchema.getType()) {
      case RECORD:
        return traverseRecord(object, defaultValue, currentSchema, targetSchema, parentName, filter);
      case UNION:
        return traverseUnion(object, defaultValue, currentSchema, targetSchema, parentName, filter);
      case ARRAY:
        return traverseArray(object, defaultValue, currentSchema, targetSchema, parentName, filter);
      case MAP:
        return traverseMap(object, defaultValue, currentSchema, targetSchema, parentName, filter);
      default:
        return applyFilter(filter, object, parentName, defaultValue, currentSchema, targetSchema);
    }
  }

  /**
   * Traverse the record object
   * @param object the object to be traversed
   * @param defaultValue the default value of the object derived from the schema
   * @param currentSchema the current schema of the object
   * @param targetSchema the target schema to be compared with
   * @param parentName the parent name of the object
   * @param filter the filter function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  private boolean traverseRecord(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter) {

    if (targetSchema == null)
      return applyFilter(filter, object, parentName, defaultValue, currentSchema, null);

    for (Schema.Field field: currentSchema.getFields()) {
      String fieldName = buildFieldPath(parentName, field.name());
      Schema.Field targetField = targetSchema.getField(field.name());
      Object value = ((GenericRecord) object).get(field.name());

      if (targetField == null) {
        if (applyFilter(filter, value, fieldName, field.defaultVal(), field.schema(), null))
          return true;
      } else if (isNestedType(field.schema())) {
        if (traverse(value, field.defaultVal(), field.schema(), targetField.schema(), fieldName, filter))
          return true;
      } else {
        if (applyFilter(filter, value, fieldName, field.defaultVal(), field.schema(), targetField.schema()))
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
   * @param filter the filter function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  private boolean traverseArray(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter) {

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
            filter))
          return true;
      }
    }
    return applyFilter(
        filter,
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
   * @param filter the filter function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  private boolean traverseMap(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter) {
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
            filter))
          return true;
      }
      return false;
    }
    return applyFilter(
        filter,
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
   * @param filter the filter function to validate the object
   * @return true if the object is invalid, false otherwise
   */
  private boolean traverseUnion(
      Object object,
      Object defaultValue,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter) {

    String objectName = ((GenericRecord) object).getSchema().getName();
    String fieldPath = buildFieldPath(parentName, objectName);

    for (Schema subCurrentSchema: currentSchema.getTypes()) {
      if (!subCurrentSchema.getName().equals(objectName))
        continue;
      boolean found = false;
      for (Schema subTargetSchema: targetSchema.getTypes()) {
        if (subCurrentSchema.getFullName().equals(subTargetSchema.getFullName())) {
          found = true;
          if (traverse(object, defaultValue, subCurrentSchema, subTargetSchema, fieldPath, filter))
            return true;
          break;
        }
      }
      if (!found && applyFilter(filter, object, fieldPath, defaultValue, subCurrentSchema, null))
        return true;
    }
    return false;
  }

  /**
   * Apply the filter function to the object
   * @param filter the filter function to validate the object
   * @param object the object to be validated
   * @param fieldName the field name of the object
   * @param fieldDefaultValue the default value of the field derived from the schema
   * @param currentSchema the current schema of the object
   * @param targetSchema the target schema to be compared with
   * @return true if the object is invalid, false otherwise
   */
  private boolean applyFilter(
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter,
      Object object,
      String fieldName,
      Object fieldDefaultValue,
      Schema currentSchema,
      Schema targetSchema) {
    return filter.apply(
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
    return new Schema.Field(name, schema, "", defaultValue);
  }

  private static String buildFieldPath(String parent, String field) {
    return parent.isEmpty() ? field : parent + "_" + field;
  }

  public BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> createSemanticCheck(
      AtomicReference<String> errorMessage) {
    return (object, schemasPair) -> {
      Schema.Field currentField = schemasPair.getFirst();
      Schema.Field targetField = schemasPair.getSecond();

      if (currentField == null || object == null)
        return false;

      if (targetField == null) {
        if (isNestedType(currentField.schema())) {
          errorMessage.set(craftErrorMessage(currentField, object));
          return true;
        }
        if (isNonDefaultValue(object, currentField, errorMessage)) {
          errorMessage.set(craftErrorMessage(currentField, object));
          return true;
        }
        return false;
      } else {
        if (currentField.schema().getType() != targetField.schema().getType()) {
          errorMessage.set("Field: " + currentField.name() + " has different types");
          return true;
        }

        if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(currentField.schema(), targetField.schema()))
          return false;

        switch (currentField.schema().getType()) {
          case ENUM:
            if (isNewEnumValue(object, currentField, targetField, errorMessage))
              return true;
            break;
          case FIXED:
            break;
          default:
            if (isNonDefaultValue(object, currentField, errorMessage))
              return true;
            break;
        }
      }
      return false;
    };
  }

  private boolean isNewEnumValue(
      Object object,
      Schema.Field currentField,
      Schema.Field targetField,
      AtomicReference<String> errorMessage) {
    List<String> currentSymbols = currentField.schema().getEnumSymbols();
    List<String> targetSymbols = targetField.schema().getEnumSymbols();
    if (targetSymbols.stream().anyMatch(symbol -> !currentSymbols.contains(symbol) && symbol.equals(object))) {
      errorMessage.set(craftErrorMessage(currentField, object));
      return true;
    }
    ;
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
    if (!value.equals(defaultValues.getOrDefault(field.schema().getType(), null))
        && !value.equals(field.defaultVal())) {
      errorMessage.set(craftErrorMessage(field, value));
      return true;
    }
    return false;
  }

  private String craftErrorMessage(Schema.Field field, Object value) {
    return "Field: " + field.name().replace("_", ".") + " is a non-default value. Actual value: " + value
        + ". Default value: " + field.defaultVal();
  }
}
