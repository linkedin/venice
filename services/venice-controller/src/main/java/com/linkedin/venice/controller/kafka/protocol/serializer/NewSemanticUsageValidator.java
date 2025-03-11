package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.Pair;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Provides a semantic validation function to check whether an object adheres to a given schema.
 * An object is considered valid if:
 * <ol>
 *   <li>It is null.</li>
 *   <li>It is a nested type and equals its schema's default value.</li>
 *   <li>It is a non-nested field and matches either the schema's default value or its type's default.</li>
 * </ol>
 *
 * If the object is not using new semantic, we return False. Otherwise, we return True along with the message
 * in errorMessage. The traverser should stop traversing the object if the return value is True.
 */
public class NewSemanticUsageValidator {
  private static final ThreadLocal<String> errorMessage = ThreadLocal.withInitial(() -> "");

  /**
   * This is the validator function that we will pass into SchemaDiffTraverser to validate the two nodes
   * with different schemas.
   */
  private final BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> SEMANTIC_VALIDATOR =
      (object, schemasPair) -> {
        Schema.Field currentField = schemasPair.getFirst();
        Schema.Field targetField = schemasPair.getSecond();

        // If object is null, return false, no need to validate more
        if (object == null) {
          return false;
        }

        // Default behavior, check if the value is non-default
        return isNonDefaultValue(object, currentField, targetField);
      };

  /**
   * Get the semantic validator function.
   */
  public BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> getSemanticValidator() {
    return SEMANTIC_VALIDATOR;
  }

  /**
   * General method to check if the value is non-default for the given field.
   * @param object the value to check
   * @param currentField the field to check
   * @param targetField the target field to check
   * @return true if the value is non-default, false otherwise
   */
  public boolean isNonDefaultValue(Object object, Schema.Field currentField, Schema.Field targetField) {
    switch (currentField.schema().getType()) {
      case UNION:
        return isNonDefaultValueUnion(object, currentField);
      case ENUM:
        return isNewEnumValue(object, currentField, targetField);
      case FIXED:
        if (currentField.schema().getFixedSize() != targetField.schema().getFixedSize()) {
          return returnTrueAndLogError(
              String.format("Field %s: has different fixed size", formatFieldName(currentField.name())));
        }
        return false;
      default:
        return isNonDefaultValueField(object, currentField);
    }
  }

  /**
   * Check if the value is a new enum value.
   * @param object the value to check
   * @param currentField the field to check with enum type and its current symbols
   * @param targetField the field to check with enum type and its target symbols
   * @return true if the value is a new enum value, false otherwise
   */
  public boolean isNewEnumValue(Object object, Schema.Field currentField, Schema.Field targetField) {
    List<String> currentSymbols = currentField.schema().getEnumSymbols();
    List<String> targetSymbols = targetField.schema().getEnumSymbols();
    if (targetSymbols.stream().anyMatch(symbol -> !currentSymbols.contains(symbol) && symbol.equals(object))) {
      return returnTrueAndLogError(
          String.format("Field %s contains new enum value: %s", formatFieldName(currentField.name()), object));
    }
    return false;
  }

  /**
   * Check if the value is non-default for the given union field.
   * If the value is null, it is considered default.
   * If object is non-null, we need to find the schema for the object inside the union and check the default value.
   * @param object the value to check
   * @param currentField the field to check with union type
   * @return true if the value is non-default, false otherwise
   */
  public boolean isNonDefaultValueUnion(Object object, Schema.Field currentField) {
    if (object == null) {
      return false;
    }

    Schema subSchema = findObjectSchemaInsideUnion(object, currentField);
    if (subSchema == null) {
      return returnTrueAndLogError(
          String.format(
              "Field %s: Cannot find the match schema for value %s from schema union",
              formatFieldName(currentField.name()),
              object));
    }

    Schema.Field newCurrentField = AvroCompatibilityHelper.createSchemaField(currentField.name(), subSchema, "", null);
    return isNonDefaultValueField(object, newCurrentField);
  }

  /**
   * Find the schema for the object inside the union.
   * @param currentField the field with union type
   * @param object the object to find the schema
   * @return the schema for the object if found, null otherwise
   */
  public static Schema findObjectSchemaInsideUnion(Object object, Schema.Field currentField) {
    for (Schema subSchema: currentField.schema().getTypes()) {
      if (isCorrectSchema(object, subSchema)) {
        return subSchema;
      }
    }
    return null;
  }

  /**
   * Check if the value is non-default for the given field.
   * If the value is null, it is considered default.
   * Default value of each type are only applicable for non-nested fields.
   * If the field is nested-field, the default value is always null.
   *
   * @param object: the value to check
   * @param field: the field to check
   * @return true if the value is non-default, false otherwise
   */
  public boolean isNonDefaultValueField(Object object, Schema.Field field) {
    if (object == null)
      return false;

    Object fieldDefaultValue = AvroSchemaUtils.getFieldDefault(field);
    Object typeDefaultValue = getDefaultForType(field.schema().getType());

    if (!Objects.equals(object, typeDefaultValue) && !Objects.equals(object, fieldDefaultValue)) {
      return returnTrueAndLogError(
          String.format(
              "Field: %s contains non-default value. Actual value: %s. Default value: %s or %s",
              formatFieldName(field.name()),
              object,
              fieldDefaultValue,
              typeDefaultValue));
    }
    return false;
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
   * Format the field name to replace "_" with ".". Reason is that we use "_" as a delimiter in the field name since "." is
   * invalid character in field name.
   */
  private static String formatFieldName(String fieldName) {
    return fieldName.replace("_", ".");
  }

  /**
   * Get the default value for the given type.
   * @param type the type
   * @return the default value if it is primitive type, else null for nested fields.
   */
  private static Object getDefaultForType(Schema.Type type) {
    switch (type) {
      case STRING:
        return "";
      case BOOLEAN:
        return false;
      case INT:
        return 0;
      case LONG:
        return 0L;
      case FLOAT:
        return 0.0f;
      case DOUBLE:
        return 0.0d;
      case BYTES:
        return new byte[0];
      default:
        return null;
    }
  }

  /**
   * Check if the value matches correct schema type.
   */
  private static boolean isCorrectSchema(Object value, Schema schema) {
    if (value == null)
      return true;

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
        return schema.hasEnumSymbol(value.toString());
      case FIXED:
        return (value instanceof byte[]) && ((byte[]) value).length == schema.getFixedSize();
      case BYTES:
        return value instanceof byte[];
      case RECORD:
        return value instanceof GenericRecord;
      case ARRAY:
        return value instanceof List;
      case MAP:
        return value instanceof Map;
      case NULL:
        return false; // since object != null
      default:
        throw new IllegalArgumentException("Unsupported schema type: " + schema.getType());
    }
  }

  /**
   * Get the error message.
   */
  public String getErrorMessage() {
    return errorMessage.get();
  }
}
