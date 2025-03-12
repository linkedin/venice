package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * SemanticDetector is used to detect the semantic changes between two schemas.
 * It traverses the object based on the current schema and target schema.
 * It returns true if there is a semantic change between the two schemas & value from @code{@param object} is non-default value.
 * It returns false if there is no semantic change between the two schemas OR value from @code{@param object} is default value.
 *
 * Semantic changes include:
 * 1. Changing the type of a field
 * 2. Adding a new field
 * 3. Changing the fixed size of a fixed type
 * 4. Adding a new enum value
 */
public class SemanticDetector {
  private static final ThreadLocal<String> errorMessage = ThreadLocal.withInitial(() -> "");

  /**
   * Traverse the object based on the current schema and target schema. Stop traversing if found a semantic change & using not default value.
   * @param object the object containing values
   * @param currentSchema the object schema
   * @param targetSchema the target schema we want to compare with
   * @param name The name of the object from the parent object
   * @param defaultValue The default value of the object
   * @return true if there is a semantic change between the two schemas & value from @code{@param object} is non-default value.
   *        false if there is no semantic change between the two schemas OR value from @code{@param object} is default value.
   */
  public boolean traverse(Object object, Schema currentSchema, Schema targetSchema, String name, Object defaultValue) {
    // If target schema is null, this is a new field and we need to validate the object against the current schema.
    if (targetSchema == null) {
      return validate(object, currentSchema, name, defaultValue);
    }

    // If the current schema and target schema do not have the same type, we return true. This is a semantic change.
    if (currentSchema.getType() != targetSchema.getType()) {
      return returnTrueAndLogError(
          String.format(
              "Field %s: Type %s is not the same as %s",
              formatFieldName(name),
              currentSchema.getType(),
              targetSchema.getType()));
    }

    // If the current schema and target schema are the same, we do not need to traverse the object.
    // @code{compareSchemaIgnoreFieldOrder} method is used to compare the two schemas (but cannot take FIXED type)
    if (currentSchema.getType() != Schema.Type.FIXED
        && AvroSchemaUtils.compareSchemaIgnoreFieldOrder(currentSchema, targetSchema))
      return false;

    switch (currentSchema.getType()) {
      case RECORD:
        return traverseFields((GenericRecord) object, currentSchema, targetSchema, name, defaultValue);
      case ARRAY:
        return traverseCollections((List<Object>) object, currentSchema, targetSchema, name, defaultValue);
      case MAP:
        return traverseMap((Map<String, Object>) object, currentSchema, targetSchema, name, defaultValue);
      case UNION:
        return traverseUnion(object, currentSchema, targetSchema, name, defaultValue);
      case ENUM:
        // If object is using new enum value, return true. This object is using new semantic.
        return validateEnum(object, currentSchema, targetSchema, name);
      case FIXED:
        // If the fixed size is different, return true. This is a semantic change.
        if (currentSchema.getFixedSize() != targetSchema.getFixedSize()) {
          return returnTrueAndLogError(
              String.format("Field %s: Changing fixed size is not allowed.", formatFieldName(name)));
        }
        return false;
      default:
        return false;
    }
  }

  /**
   * Traverse the fields of the record. Stop traversing if found a semantic change & using not default value.
   * We want to loop through fields to find the mismatch field between the current schema and target schema.
   * <p>
   * For example,
   * <ul>
   *   <li>Current schema: field1, field2, field3</li>
   *   <li>Target schema: field1, field2</li>
   * </ul>
   *
   * We want to find the mismatch field field3 between the two schemas => field3 is a new field.
   * And validate the value of field3 against the current schema.
   * </p>
   * @param record the record containing values
   * @param currentSchema the record schema
   * @param targetSchema the target schema we want to compare with
   * @param recordName The name of the record from the parent object
   * @param defaultValue The default value of the record
   * @return true if there is a semantic change between the two schemas & value from @code{@param record} is non-default value.
   *       false if there is no semantic change between the two schemas OR value from @code{@param record} is default value.
   */
  public boolean traverseFields(
      GenericRecord record,
      Schema currentSchema,
      Schema targetSchema,
      String recordName,
      Object defaultValue) {
    for (Schema.Field field: currentSchema.getFields()) {
      String fieldName = buildFieldPath(recordName, field.name());
      Schema.Field targetField = targetSchema.getField(field.name());
      Object value = record.get(field.name());
      Object fieldDefaultValue = AvroSchemaUtils.getFieldDefault(field);
      Schema fieldSchema = field.schema();

      if (targetField == null) {
        if (traverse(value, fieldSchema, null, fieldName, fieldDefaultValue)) {
          return true;
        }
        continue;
      }

      if (traverse(value, fieldSchema, targetField.schema(), fieldName, fieldDefaultValue)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Traverse the array to make sure the array elements are not using new semantic.
   * <p>
   *   For example,
   *   <ul>
   *     <li>Current schema: List (RecordA - field1, field2, field3) </li>
   *     <li>Target schema: List (RecordA - field1, field2) </li>
   *   </ul>
   *   In this case, we want to check every element in the array to make sure the element is not using new semantic.
   * </p>
   *
   * @param array the array containing values
   * @param currentSchema the array schema
   * @param targetSchema the target schema we want to compare with
   * @param name The name of the array from the parent object
   * @param defaultArrayEntry The default value of the array
   * @return true if there is a semantic change between the two schemas & value from @code{@param array} is non-default value.
   */
  public boolean traverseCollections(
      List<Object> array,
      Schema currentSchema,
      Schema targetSchema,
      String name,
      Object defaultArrayEntry) {
    String arrayName = buildFieldPath(name, currentSchema.getName());
    String nestedSchemaName = buildFieldPath(arrayName, currentSchema.getElementType().getName());

    Schema currentElementSchema = currentSchema.getElementType();
    Schema targetElementSchema = targetSchema.getElementType();

    for (int i = 0; i < array.size(); i++) {
      String indexArrayName = buildFieldPath(nestedSchemaName, String.valueOf(i));
      if (traverse(array.get(i), currentElementSchema, targetElementSchema, indexArrayName, defaultArrayEntry)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Traverse the map to make sure the map values are not using new semantic.
   * <p>
   *   For example,
   *   <ul>
   *     <li>Current schema: Map (String, RecordA - field1, field2, field3) </li>
   *     <li>Target schema: Map (String, RecordA - field1, field2) </li>
   *   </ul>
   *   In this case, we want to check every value in the map to make sure the value is not using new semantic.
   *   We do not need to check the key since the key is a string.
   * </p>
   *
   * @param map the map containing values
   * @param currentSchema the map schema
   * @param targetSchema the target schema we want to compare with
   * @param name The name of the map from the parent object
   * @param defaultValueEntry The default value of the map
   * @return true if there is a semantic change between the two schemas & value from @code{@param map} is non-default value.
   */
  public boolean traverseMap(
      Map<String, Object> map,
      Schema currentSchema,
      Schema targetSchema,
      String name,
      Object defaultValueEntry) {
    Schema currentValueSchema = currentSchema.getValueType();
    Schema targetValueSchema = targetSchema.getValueType();
    String mapName = buildFieldPath(name, currentSchema.getName());
    String nestedMapName = buildFieldPath(mapName, currentSchema.getValueType().getName());

    for (Map.Entry<String, Object> entry: map.entrySet()) {
      if (traverse(
          entry.getValue(),
          currentValueSchema,
          targetValueSchema,
          buildFieldPath(nestedMapName, entry.getKey()),
          defaultValueEntry)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Traverse the union to make sure the object is not using new semantic.
   * <p>
   *   For example,
   *   <ul>
   *     <li>Current schema: Union (RecordA - field1, field2, field3, RecordB - field1, field2) </li>
   *     <li>Target schema: Union (RecordA - field1, field2, field3) </li>
  *     </ul>
   *     In this case, we want to check the object against the schema within the union schema.
   *     If object - RecordA(field1, field2, field3), then we are NOT using new semantic change.
   *     If object - RecordB(field1, field2), then we have a semantic change.
   *     If the object is using new semantic, we return true.
   * </p>
   * @param object the object containing values
   * @param currentSchema the union schema
   * @param targetSchema the target schema we want to compare with
   * @param name The name of the object from the parent object
   * @param defaultValue The default value of the object
   * @return true if there is a semantic change between the two schemas & value from @code{@param object} is non-default value.
   */
  public boolean traverseUnion(
      Object object,
      Schema currentSchema,
      Schema targetSchema,
      String name,
      Object defaultValue) {
    List<Schema> schemasInUnion = currentSchema.getTypes();
    Schema objectSchema = getObjectSchema(object, currentSchema);

    if (objectSchema == null) {
      return returnTrueAndLogError(
          String.format(
              "Field %s: Object %s does not match any schema within union schemas %s",
              formatFieldName(name),
              object,
              schemasInUnion));
    }

    Map<String, Schema> targetSchemaMap =
        targetSchema.getTypes().stream().collect(Collectors.toMap(s -> s.getName(), s -> s));

    Schema subTargetSchema = targetSchemaMap.get(objectSchema.getName());
    String objectName = buildFieldPath(name, objectSchema.getName());
    return traverse(object, objectSchema, subTargetSchema, objectName, defaultValue);
  }

  public Schema getObjectSchema(Object object, Schema unionSchema) {
    for (Schema schema: unionSchema.getTypes()) {
      if (isCorrectSchema(object, schema)) {
        return schema;
      }
    }
    return null;
  }

  /**
   * Check if the value matches correct schema type.
   * @param value the value to check
   * @param schema the schema to check against
   * @return true if the value matches the schema type, false otherwise
   */
  private static boolean isCorrectSchema(Object value, Schema schema) {
    switch (schema.getType()) {
      case STRING:
        return value instanceof String;
      case INT:
        return value instanceof Integer;
      case LONG:
        return value instanceof Long;
      case FLOAT:
        return value instanceof Float;
      case DOUBLE:
        return value instanceof Double;
      case BOOLEAN:
        return value instanceof Boolean;
      case ENUM:
        return value instanceof String && schema.getEnumSymbols().contains(value);
      case FIXED:
        return (value instanceof byte[]) && ((byte[]) value).length == schema.getFixedSize();
      case BYTES:
        return value instanceof byte[];
      case RECORD:
        return value instanceof GenericRecord && ((GenericRecord) value).getSchema().equals(schema);
      case ARRAY:
        return value instanceof List
            && ((List<?>) value).stream().allMatch(element -> isCorrectSchema(element, schema.getElementType()));
      case MAP:
        return value instanceof Map && ((Map<?, ?>) value).values()
            .stream()
            .allMatch(element -> isCorrectSchema(element, schema.getValueType()));
      case NULL:
        return value == null;
      default:
        throw new IllegalArgumentException("Unsupported schema type: " + schema.getType());
    }
  }

  /**
   * Validate the object with default value.
   * There are two default values:
   * <ol>
   *   <li>Default value derived from schema as @code{@param defaultValue}} </li>
   *   <li>Default value derived from the schema type of the object</li>
   * </ol>
   * @param object the object to validate
   * @param currentSchema the schema of the object
   * @param name the name of the object
   * @param defaultValue the default value of the object
   * @return true if the object is not the default value, false otherwise
   */
  public boolean validate(Object object, Schema currentSchema, String name, Object defaultValue) {
    if (object == null) {
      return false;
    }

    if (object.equals(defaultValue)) {
      return false;
    }

    switch (currentSchema.getType()) {
      case INT:
        if ((object instanceof Integer) && (int) object == 0) {
          return false;
        } else {
          return returnTrueAndLogError(
              String.format(
                  "Field %s: Integer value %s is not the default value 0 or %s",
                  formatFieldName(name),
                  object,
                  defaultValue));
        }
      case LONG:
        if ((object instanceof Long) && (long) object == 0L) {
          return false;
        } else {
          return returnTrueAndLogError(
              String.format(
                  "Field %s: Long value %s is not the default value 0 or %s",
                  formatFieldName(name),
                  object,
                  defaultValue));
        }
      case FLOAT:
        if ((object instanceof Float) && (float) object == 0.0f) {
          return false;
        } else {
          return returnTrueAndLogError(
              String.format(
                  "Field %s: Float value %s is not the default value 0.0 or %s",
                  formatFieldName(name),
                  object,
                  defaultValue));
        }
      case DOUBLE:
        if ((object instanceof Double) && (double) object == 0.0) {
          return false;
        } else {
          return returnTrueAndLogError(
              String.format(
                  "Field %s: Double value %s is not the default value 0.0 or %s",
                  formatFieldName(name),
                  object,
                  defaultValue));
        }
      case BOOLEAN:
        if ((object instanceof Boolean) && !(boolean) object) {
          return false;
        } else {
          return returnTrueAndLogError(
              String.format(
                  "Field %s: Boolean value %s is not the default value false or %s",
                  formatFieldName(name),
                  object,
                  defaultValue));
        }
      case STRING:
        if ((object instanceof String) && ((String) object).isEmpty()) {
          return false;
        } else {
          return returnTrueAndLogError(
              String.format(
                  "Field %s: String value %s is not the default value \"\" or %s",
                  formatFieldName(name),
                  object,
                  defaultValue));
        }
      case BYTES:
        if ((object instanceof byte[]) && ((byte[]) object).length == 0) {
          return false;
        } else {
          return returnTrueAndLogError(
              String.format(
                  "Field %s: Bytes value %s is not the default value [] or %s",
                  formatFieldName(name),
                  object,
                  defaultValue));
        }
      default:
        return returnTrueAndLogError(
            String.format(
                "Field %s: Value %s doesn't match default value %s",
                formatFieldName(name),
                object,
                defaultValue));
    }
  }

  public boolean validateEnum(Object enumValue, Schema currentSchema, Schema targetSchema, String name) {
    if (!(enumValue instanceof String)) {
      return returnTrueAndLogError(
          String.format("Field %s: Enum value %s is not a string", formatFieldName(name), enumValue));
    }

    List<String> prevEnumSymbols = currentSchema.getEnumSymbols();
    List<String> targetEnumSymbols = targetSchema.getEnumSymbols();
    // If the value is not in the previous enum symbols but in the target enum symbols, it is a new enum value
    if (!prevEnumSymbols.contains(enumValue) && targetEnumSymbols.contains(enumValue)) {
      return returnTrueAndLogError(
          String.format(
              "Field %s: Enum value %s is not in the previous enum symbols but in the target enum symbols",
              formatFieldName(name),
              enumValue));
    }
    return false;
  }

  private static String buildFieldPath(String parent, String field) {
    return parent.isEmpty() ? field : parent + "_" + field;
  }

  /**
   * Format the field name to replace "_" with ".". Reason is that we use "_" as a delimiter in the field name since "." is
   * invalid character in field name.
   */
  private static String formatFieldName(String fieldName) {
    return fieldName.replace("_", ".");
  }

  /**
   * Return true and log the error message.
   * @param message the error message
   */
  private boolean returnTrueAndLogError(String message) {
    errorMessage.set(message);
    return true;
  }

  /**
   * Get the error message.
   */
  public String getErrorMessage() {
    return errorMessage.get();
  }
}
