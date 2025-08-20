package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.venice.exceptions.VeniceProtocolException;
import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * SemanticDetector is used to detect the semantic changes between two schemas.
 * It traverses the object based on the current schema and target schema.
 * When we detect new semantic change, we compare object with default value.
 * If the object is NOT default value, we throw @code{VeniceProtocolException}.
 *
 * Semantic changes include:
 * 1. Changing the type of a field
 * 2. Adding a new field
 * 3. Changing the fixed size of a fixed type
 * 4. Adding a new enum value
 */
public class SemanticDetector {
  /**
   * Traverse the object based on the current schema and target schema. Stop traversing if found a semantic change &
   * using not default value.
   *
   * @param object        the object containing values
   * @param currentSchema the object schema
   * @param targetSchema  the target schema we want to compare with
   * @param name          The name of the object from the parent object
   * @param defaultValue  The default value of the object
   * @throws VeniceProtocolException if there is a semantic change between the two schemas & value from
   *                                 @code{@param object} is non-default value.
   */
  public static void traverseAndValidate(
      Object object,
      Schema currentSchema,
      Schema targetSchema,
      String name,
      Object defaultValue) {
    // If target schema is null, this is a new field and we need to validate the object against the current schema.
    if (targetSchema == null) {
      compareObjectToDefaultValue(object, currentSchema, name, defaultValue);
      return;
    }

    // If the current schema and target schema do not have the same type, we return true. This is a semantic change.
    if (currentSchema.getType() != targetSchema.getType()) {
      throw new VeniceProtocolException(
          String.format(
              "Field %s: Type %s is not the same as %s",
              name,
              currentSchema.getType(),
              targetSchema.getType()));
    }

    // If the current schema and target schema are the same, we do not need to traverse the object.
    // @code{compareSchemaIgnoreFieldOrder} method is used to compare the two schemas (but cannot take FIXED type)
    // If two schemas are FIXED, we need to check the fixed size to make sure they are the same.
    if (currentSchema.getType() != Schema.Type.FIXED
        && AvroSchemaUtils.compareSchemaIgnoreFieldOrder(currentSchema, targetSchema))
      return;

    switch (currentSchema.getType()) {
      case RECORD:
        traverseFields((GenericRecord) object, currentSchema, targetSchema, name, defaultValue);
        break;
      case ARRAY:
        traverseCollections((List<Object>) object, currentSchema, targetSchema, name, defaultValue);
        break;
      case MAP:
        traverseMap((Map<String, Object>) object, currentSchema, targetSchema, name, defaultValue);
        break;
      case UNION:
        traverseUnion(object, currentSchema, targetSchema, name, defaultValue);
        break;
      case ENUM:
        // If object is using new enum value, throw exception.
        validateEnum(object, currentSchema, targetSchema, name);
        break;
      case FIXED:
        // If the fixed size is different, throw exception. Fixed size change is a new semantic change, and should not
        // be allowed.
        if (currentSchema.getFixedSize() != targetSchema.getFixedSize()) {
          throw new VeniceProtocolException(String.format("Field %s: Changing fixed size is not allowed.", name));
        }
        break;
      default:
        // For primitive types, the only diff in schemas is converting field to optional (adding default)
        // In this case, we still can serve the value in target schema. Consider it as no semantic change.
        break;
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
   * @throws VeniceProtocolException if there is a semantic change between the two schemas & field value from @code{@param record} is non-default value.
   */
  public static void traverseFields(
      GenericRecord record,
      Schema currentSchema,
      Schema targetSchema,
      String recordName,
      Object defaultValue) {
    for (Schema.Field field: currentSchema.getFields()) {
      String fieldName = buildFieldPath(recordName, field.name());
      Schema.Field targetField = targetSchema.getField(field.name());
      Object value = record.get(field.name());
      traverseAndValidate(
          value,
          field.schema(),
          targetField == null ? null : targetField.schema(),
          fieldName,
          AvroSchemaUtils.getFieldDefault(field));
    }
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
   * @throws VeniceProtocolException if there is a semantic change between the two schemas & element from @code{@param array} is non-default value.
   */
  public static void traverseCollections(
      List<Object> array,
      Schema currentSchema,
      Schema targetSchema,
      String name,
      Object defaultArrayEntry) {
    String arrayName = buildFieldPath(name, currentSchema.getName());
    String elementName = currentSchema.getElementType().getName();

    Schema currentElementSchema = currentSchema.getElementType();
    Schema targetElementSchema = targetSchema.getElementType();

    for (int i = 0; i < array.size(); i++) {
      String indexArrayName = buildFieldPath(arrayName, String.valueOf(i), elementName);
      traverseAndValidate(array.get(i), currentElementSchema, targetElementSchema, indexArrayName, defaultArrayEntry);
    }
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
   * @throws VeniceProtocolException if there is a semantic change between the two schemas & value from @code{@param map} is non-default value.
   */
  public static void traverseMap(
      Map<String, Object> map,
      Schema currentSchema,
      Schema targetSchema,
      String name,
      Object defaultValueEntry) {
    Schema currentValueSchema = currentSchema.getValueType();
    Schema targetValueSchema = targetSchema.getValueType();
    String mapName = buildFieldPath(name, currentSchema.getName());
    String valueObjectName = currentSchema.getValueType().getName();

    for (Map.Entry<String, Object> entry: map.entrySet()) {
      traverseAndValidate(
          entry.getValue(),
          currentValueSchema,
          targetValueSchema,
          buildFieldPath(mapName, entry.getKey(), valueObjectName),
          defaultValueEntry);
    }
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
   * @throws VeniceProtocolException if there is a semantic change between the two schemas & value from @code{@param object} is non-default value
   * OR if the object does not match any schema within the union schemas.
   */
  public static void traverseUnion(
      Object object,
      Schema currentSchema,
      Schema targetSchema,
      String name,
      Object defaultValue) {
    List<Schema> schemasInUnion = currentSchema.getTypes();
    Schema objectSchema = getObjectSchema(object, currentSchema);

    if (objectSchema == null) {
      throw new VeniceProtocolException(
          String.format(
              "Field %s: Object %s does not match any schema within union schemas %s",
              name,
              object,
              schemasInUnion));
    }

    Map<String, Schema> targetSchemaMap =
        targetSchema.getTypes().stream().collect(Collectors.toMap(Schema::getName, s -> s));

    Schema subTargetSchema = targetSchemaMap.get(objectSchema.getName());
    String objectName = buildFieldPath(name, objectSchema.getName());
    traverseAndValidate(object, objectSchema, subTargetSchema, objectName, defaultValue);
  }

  public static Schema getObjectSchema(Object object, Schema unionSchema) {
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
            && ((List<?>) value).stream().anyMatch(element -> isCorrectSchema(element, schema.getElementType()));
      case MAP:
        return value instanceof Map && ((Map<?, ?>) value).values()
            .stream()
            .anyMatch(element -> isCorrectSchema(element, schema.getValueType()));
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
   */
  public static void compareObjectToDefaultValue(
      Object object,
      Schema currentSchema,
      String name,
      Object defaultValue) {
    if (object == null || object.equals(defaultValue)) {
      return;
    }

    switch (currentSchema.getType()) {
      case INT:
        if (!((object instanceof Integer) && (int) object == 0)) {
          throw new VeniceProtocolException(
              String.format("Field %s: Integer value %s is not the default value 0 or %s", name, object, defaultValue));
        }
        break;
      case LONG:
        if (!((object instanceof Long) && (long) object == 0L)) {
          throw new VeniceProtocolException(
              String.format("Field %s: Long value %s is not the default value 0 or %s", name, object, defaultValue));
        }
        break;
      case FLOAT:
        if (!((object instanceof Float) && Math.abs(((float) object) - 0.0f) < 0.00001)) {
          throw new VeniceProtocolException(
              String.format("Field %s: Float value %s is not the default value 0.0 or %s", name, object, defaultValue));
        }
        break;
      case DOUBLE:
        if (!((object instanceof Double) && Math.abs(((double) object) - 0.0) < 0.00001)) {
          throw new VeniceProtocolException(
              String
                  .format("Field %s: Double value %s is not the default value 0.0 or %s", name, object, defaultValue));
        }
        break;
      case BOOLEAN:
        if (!((object instanceof Boolean) && !(boolean) object)) {
          throw new VeniceProtocolException(
              String.format(
                  "Field %s: Boolean value %s is not the default value false or %s",
                  name,
                  object,
                  defaultValue));
        }
        break;
      case STRING:
        // For string with default not-empty value, the default value type is org.apache.avro.util.Utf8,
        // the above defaultValue equal check will not work for utf8 and String comparison.
        if (object instanceof String && String.valueOf(object).equals(String.valueOf(defaultValue))) {
          return;
        }

        if (!((object instanceof String) && ((String) object).isEmpty())) {
          throw new VeniceProtocolException(
              String
                  .format("Field %s: String value %s is not the default value \"\" or %s", name, object, defaultValue));
        }
        break;
      case BYTES:
        if (!((object instanceof byte[]) && ((byte[]) object).length == 0)) {
          throw new VeniceProtocolException(
              String.format("Field %s: Bytes value %s is not the default value [] or %s", name, object, defaultValue));
        }
        break;
      default:
        throw new VeniceProtocolException(
            String.format(
                "Field %s: Value %s doesn't match default value %s",
                name,
                object instanceof String && ((String) object).isEmpty() ? "\"\"" : object,
                defaultValue));
    }
  }

  /**
   * Validate the enum value.
   * If the enum value is in current schema but NOT in target schema, it is a new enum value.
   * @param enumValue the enum value to validate
   * @param currentSchema the current schema
   * @param targetSchema the target schema
   * @param name the name of the enum value
   * @throws VeniceProtocolException if the enum value is not in the target schema OR if the enum value is not a string
   */
  public static void validateEnum(Object enumValue, Schema currentSchema, Schema targetSchema, String name) {
    if (!(enumValue instanceof String)) {
      throw new VeniceProtocolException(String.format("Field %s: Enum value %s is not a string", name, enumValue));
    }

    List<String> prevEnumSymbols = currentSchema.getEnumSymbols();
    List<String> targetEnumSymbols = targetSchema.getEnumSymbols();
    // Not valid value due to the mismatch with current schema
    if (!prevEnumSymbols.contains(enumValue)) {
      throw new VeniceProtocolException(
          String.format(
              "Field %s: Invalid enum value %s is not accepted in the current schema %s",
              name,
              enumValue,
              prevEnumSymbols));
    }

    // If the value is in the previous enum symbols but NOT in the target enum symbols, it is a new enum value
    if (!targetEnumSymbols.contains(enumValue)) {
      throw new VeniceProtocolException(
          String.format(
              "Field %s: Enum value %s is not accepted in the target schema %s",
              name,
              enumValue,
              targetEnumSymbols));
    }
  }

  /**
   * Format the field name.
   * @param parts the parts of the field name
   * @return the formatted field name
   */
  private static String buildFieldPath(String... parts) {
    return String.join(
        ".",
        Arrays.stream(parts)
            .filter(part -> part != null && !part.isEmpty()) // Skip empty or null parts
            .toArray(String[]::new));
  }
}
