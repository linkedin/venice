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


public class SchemaDiffTraverser {
  public boolean traverse(
      Object object,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter) {

    if (object == null)
      return false;
    if (targetSchema == null)
      return applyFilter(filter, object, parentName, currentSchema, null);
    if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(currentSchema, targetSchema))
      return false;

    switch (currentSchema.getType()) {
      case RECORD:
        return traverseRecord(object, currentSchema, targetSchema, parentName, filter);
      case UNION:
        return traverseUnion(object, currentSchema, targetSchema, parentName, filter);
      case ARRAY:
        return traverseArray(object, currentSchema, targetSchema, parentName, filter);
      case MAP:
        return traverseMap(object, currentSchema, targetSchema, parentName, filter);
      default:
        return applyFilter(filter, object, parentName, currentSchema, targetSchema);
    }
  }

  private boolean traverseRecord(
      Object object,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter) {

    for (Schema.Field field: currentSchema.getFields()) {
      String fieldName = buildFieldPath(parentName, field.name());
      Schema.Field targetField = targetSchema.getField(field.name());
      Object value = ((GenericRecord) object).get(field.name());

      if (targetField == null) {
        if (applyFilter(filter, value, fieldName, field.schema(), null))
          return true;
      } else if (isNestedType(field.schema())) {
        if (traverse(value, field.schema(), targetField.schema(), fieldName, filter))
          return true;
      } else {
        if (applyFilter(filter, value, fieldName, field.schema(), targetField.schema()))
          return true;
      }
    }
    return false;
  }

  private boolean traverseArray(
      Object object,
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
            currentSchema.getElementType(),
            targetSchema.getElementType(),
            buildFieldPath(nestedSchemaName, String.valueOf(i)),
            filter))
          return true;
      }
    }
    return applyFilter(filter, object, arrayName, currentSchema.getElementType(), targetSchema.getElementType());
  }

  private boolean traverseMap(
      Object object,
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
            currentSchema.getValueType(),
            targetSchema.getValueType(),
            buildFieldPath(nestedMapName, entry.getKey()),
            filter))
          return true;
      }
      return false;
    }
    return applyFilter(filter, object, mapName, currentSchema.getValueType(), targetSchema.getValueType());
  }

  private boolean traverseUnion(
      Object object,
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
          if (traverse(object, subCurrentSchema, subTargetSchema, fieldPath, filter))
            return true;
          break;
        }
      }
      if (!found && applyFilter(filter, object, fieldPath, subCurrentSchema, null))
        return true;
    }
    return false;
  }

  private boolean applyFilter(
      BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter,
      Object object,
      String fieldName,
      Schema currentSchema,
      Schema targetSchema) {
    return filter.apply(
        object,
        new Pair<>(
            generateField(fieldName, currentSchema),
            targetSchema == null ? null : generateField(fieldName, targetSchema)));
  }

  private boolean isNestedType(Schema schema) {
    return EnumSet.of(Schema.Type.RECORD, Schema.Type.UNION, Schema.Type.ARRAY, Schema.Type.MAP)
        .contains(schema.getType());
  }

  private static Schema.Field generateField(String name, Schema schema) {
    return new Schema.Field(name, schema, "", null);
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
          errorMessage.set(craftErrorMessage(currentField.name(), object));
          return true;
        }
        if (isNonDefaultValue(object, currentField, errorMessage))
          return true;
      } else {
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
      errorMessage.set(craftErrorMessage(currentField.name(), object));
      return true;
    }
    ;
    return false;
  }

  private boolean isNonDefaultValue(Object value, Schema.Field field, AtomicReference<String> errorMessage) {
    Map<Schema.Type, Object> defaultValues = new HashMap<>();
    defaultValues.put(Schema.Type.STRING, "");
    defaultValues.put(Schema.Type.BOOLEAN, false);
    defaultValues.put(Schema.Type.INT, 0);
    defaultValues.put(Schema.Type.LONG, 0L);
    defaultValues.put(Schema.Type.FLOAT, 0.0f);
    defaultValues.put(Schema.Type.DOUBLE, 0.0d);
    defaultValues.put(Schema.Type.BYTES, new byte[0]);
    if (value != null && !value.equals(defaultValues.getOrDefault(field.schema().getType(), null))
        && !value.equals(field.defaultVal())) {
      errorMessage.set(craftErrorMessage(field.name(), value));
      return true;
    }
    ;
    return false;
  }

  private String craftErrorMessage(String fieldName, Object value) {
    return "Field: " + fieldName + " is a non-default value. Actual value: " + value;
  }
}
