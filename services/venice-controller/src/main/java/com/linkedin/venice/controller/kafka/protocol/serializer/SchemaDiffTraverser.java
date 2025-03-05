package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.avroutil1.compatibility.shaded.org.apache.commons.lang3.function.TriFunction;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class SchemaDiffTraverser {
  /**
   * Traverses the current schema and target schema, applying the filter to each leaf node if the schema is different
   * of the current schema and target schema.
   * @param object The object holding the value
   * @param currentSchema The current schema of the object
   * @param targetSchema The target schema we want to compare to
   * @param parentName The name of the field, at the current stage, it becomes parentName if we need to loop in the nested field
   * @param filter The function that we can dynamically pass in to do our job.
   */
  public void traverse(
      Object object,
      Schema currentSchema,
      Schema targetSchema,
      String parentName,
      TriFunction<Object, String, Pair<Schema.Field, Schema.Field>, Object> filter) {
    // if object is null, do nothing
    if (object == null) {
      return;
    }
    // if target schema is null, this is leaf node
    if (targetSchema == null) {
      filter.apply(object, parentName, new Pair<>(generateField(parentName, currentSchema), null));
      return;
    }
    // if current schema and target schema are the same, do nothing
    if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(currentSchema, targetSchema)) {
      return;
    }
    // at this stage, current schema and target schema are not the same
    // we will traverse the current schema and target schema, stop at non-traversable fields
    // traversable fields types are: RECORD, UNION, ARRAY, MAP
    switch (currentSchema.getType()) {
      case RECORD:
        for (Schema.Field field: currentSchema.getFields()) {
          String fieldName = buildFieldPath(parentName, field.name());
          Schema.Field targetField = targetSchema.getField(field.name());
          Object value = ((GenericRecord) object).get(field.name());
          if (targetField == null) {
            // there is no corresponding field in target schema => leaf node
            filter.apply(value, fieldName, new Pair<>(field, null));
            continue;
          }
          if (isNestedField(field.schema().getType())) {
            // if the field is a nested field, traverse it
            traverse(value, field.schema(), targetField.schema(), fieldName, filter);
          } else {
            // if the field is not a nested field, apply the filter
            filter.apply(value, fieldName, new Pair<>(field, targetField));
          }
        }
        break;
      case UNION:
        // get the object name of the object
        String objectName = ((GenericRecord) object).getSchema().getName();
        // build the field path
        String name = buildFieldPath(parentName, objectName);
        // iterate through the union schemas
        for (Schema subCurrentSchema: currentSchema.getTypes()) {
          boolean found = false;
          // if the union schema name does not match the object name, continue
          if (!subCurrentSchema.getName().equals(objectName)) {
            continue;
          }
          for (Schema subTargetSchema: targetSchema.getTypes()) {
            // if the two names of the union schemas match, traverse the object
            if (subCurrentSchema.getFullName().equals(subTargetSchema.getFullName())) {
              found = true;
              traverse(object, subCurrentSchema, subTargetSchema, name, filter);
              break;
            }
          }
          // if the object schema cannot be found in the target schema => leaf node
          if (!found) {
            filter.apply(object, name, new Pair<>(generateField(name, subCurrentSchema), null));
          }
        }
        break;
      case ARRAY:
        String arrayName = buildFieldPath(parentName, currentSchema.getName());
        filter.apply(
            object,
            parentName,
            new Pair<>(
                generateField(arrayName, currentSchema.getElementType()),
                generateField(arrayName, targetSchema.getElementType())));
        break;
      case MAP:
        String mapName = buildFieldPath(parentName, currentSchema.getName());
        filter.apply(
            object,
            parentName,
            new Pair<>(
                generateField(mapName, currentSchema.getValueType()),
                generateField(mapName, targetSchema.getValueType())));
        break;
      default:
        // for all other types, apply the filter
        filter.apply(
            object,
            parentName,
            new Pair<>(generateField(parentName, currentSchema), generateField(parentName, targetSchema)));
        break;
    }
  }

  /**
   * Helper method to construct field paths for nested fields.
   */
  private String buildFieldPath(String parent, String field) {
    return parent.isEmpty() ? field : parent + "_" + field;
  }

  /**
   * Helper method to check if a schema type is a nested field.
   */
  public boolean isNestedField(Schema.Type type) {
    return type == Schema.Type.RECORD || type == Schema.Type.UNION || type == Schema.Type.ARRAY
        || type == Schema.Type.MAP;
  }

  /**
   * Determines if a value is different from its default.
   */
  public boolean isNonDefaultValue(Object value, Schema.Field field) {
    System.out.println("Checking Field: " + field.name() + ", Value: " + value);
    Map<Schema.Type, Object> defaultValues = new HashMap<>();
    defaultValues.put(Schema.Type.STRING, "");
    defaultValues.put(Schema.Type.BOOLEAN, false);
    defaultValues.put(Schema.Type.INT, 0);
    defaultValues.put(Schema.Type.LONG, 0L);
    defaultValues.put(Schema.Type.FLOAT, 0.0f);
    defaultValues.put(Schema.Type.DOUBLE, 0.0d);
    defaultValues.put(Schema.Type.BYTES, new byte[0]);
    Object schemaDefaultValue = field.defaultVal();
    Object predefinedDefault = defaultValues.getOrDefault(field.schema().getType(), null);
    System.out.println(
        "Hmm, condition is: "
            + (value != null && !value.equals(predefinedDefault) && !value.equals(schemaDefaultValue)));
    return value != null && !value.equals(predefinedDefault) && !value.equals(schemaDefaultValue);
  }

  /**
   * Helper method to generate a field with a given name and schema.
   */
  private Schema.Field generateField(String fieldName, Schema schema) {
    return new Schema.Field(fieldName, schema, schema.getDoc(), null);
  }

  /**
   * The main filter function that will be used to check if the field is a non-default value.
   * @param flag: AtomicBoolean to set if the field is a non-default value
   * @param fieldName: List of fields that are non-default values
   * @return TriFunction that takes an object, field name, and a pair of schema fields
   */
  public TriFunction<Object, String, Pair<Schema.Field, Schema.Field>, Object> usingNewSemanticCheck(
      AtomicBoolean flag,
      ArrayList<String> fieldName) {
    TriFunction<Object, String, Pair<Schema.Field, Schema.Field>, Object> filter = (object, name, schemasPair) -> {
      System.out.println("Getting into filter, name: " + name);
      Schema.Field currentField = schemasPair.getFirst();
      Schema.Field targetField = schemasPair.getSecond();
      if (currentField == null || object == null) {
        return false;
      }
      if (targetField == null) {
        // if the current field is a nested field and the object is non-null, all fields in the record are not
        // acceptable
        if (isNestedField(currentField.schema().getType())) {
          flag.set(true);
          fieldName.add(currentField.name());
          return true;
        }
        if (isNonDefaultValue(object, currentField)) {
          System.out.println("Field: " + name + " is non-default value");
          fieldName.add(currentField.name());
          flag.set(true);
          return true;
        }
        return false;
      } else {
        // If two schemas are the same, we don't need to check, not likely to happen but just in case
        if (com.linkedin.venice.utils.AvroSchemaUtils
            .compareSchemaIgnoreFieldOrder(currentField.schema(), targetField.schema())) {
          return false;
        }
        switch (currentField.schema().getType()) {
          case ENUM:
            // if the current field is enum, the target field is also enum with different value set
            // we need to check whether object is the new enum value
            List<String> enumSymbols = currentField.schema().getEnumSymbols();
            List<String> targetEnumSymbols = targetField.schema().getEnumSymbols();
            List<String> differingEnumSymbols = targetEnumSymbols.stream()
                .filter(symbol -> !(enumSymbols).contains(symbol))
                .collect(Collectors.toList());
            if (differingEnumSymbols.contains(object)) {
              System.out.println("Field: " + name + " is non-default value");
              fieldName.add(currentField.name());
              flag.set(true);
              return true;
            }
            break;
          case FIXED:
            break;
          default:
            // If the current field is a Schema.Field, we need to check if the target field is a non-default value
            if (isNonDefaultValue(object, currentField)) {
              System.out.println("Field: " + name + " is non-default value");
              fieldName.add(currentField.name());
              flag.set(true);
              return true;
            }
            break;
        }
        return false;
      }
    };
    return filter;
  }
}
