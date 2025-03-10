package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.Pair;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
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
 */
public class NewSemanticUsageValidator {
  private static final ThreadLocal<String> errorMessage = ThreadLocal.withInitial(() -> "");

  private final BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> SEMANTIC_VALIDATOR =
      (object, schemasPair) -> {
        Schema.Field currentField = schemasPair.getFirst();
        Schema.Field targetField = schemasPair.getSecond();

        if (currentField == null || object == null) {
          return false;
        }

        if (targetField != null) {
          if (hasSchemaTypeMismatch(currentField, targetField)) {
            return true;
          }

          if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(currentField.schema(), targetField.schema())) {
            return false;
          }
        }
        return isNonDefaultValue(object, currentField, targetField);
      };

  public BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> getSemanticValidator() {
    return SEMANTIC_VALIDATOR;
  }

  public boolean hasSchemaTypeMismatch(Schema.Field currentField, Schema.Field targetField) {
    if (currentField.schema().getType() != targetField.schema().getType()) {
      return returnTrueAndLogError(
          String.format(
              "Field %s: Type mismatch %s vs %s",
              formatFieldName(currentField.name()),
              currentField.schema().getType(),
              targetField.schema().getType()));
    }
    return false;
  }

  /**
   * General method to check if the value is non-default for the given field.
   * @param object the value to check
   * @param currentField the field to check
   * @param targetField the target field to check
   * @return true if the value is non-default, false otherwise
   */
  private boolean isNonDefaultValue(Object object, Schema.Field currentField, Schema.Field targetField) {
    switch (currentField.schema().getType()) {
      case UNION:
        return isNonDefaultValueUnion(object, currentField, targetField);
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

  private static String formatFieldName(String fieldName) {
    return fieldName.replace("_", ".");
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
   * If targetField is not union, we expect the currentField to be a nullable union pair.
   * If targetField is union, we expect to find the right schema for object in both union.
   * @param object the value to check
   * @param currentField the field to check with union type
   * @param targetField the target field to check
   * @return true if the value is non-default, false otherwise
   */
  public boolean isNonDefaultValueUnion(Object object, Schema.Field currentField, Schema.Field targetField) {
    if (object == null) {
      return false;
    }

    if (targetField == null) {
      return isNonDefaultValueField(object, currentField);
    }

    List<Schema> subSchemas = currentField.schema().getTypes();

    // If the target field is not a union, check if the current field is a nullable union pair
    if (targetField.schema().getType() != Schema.Type.UNION) {
      if (AvroSchemaUtils.isNullableUnionPair(currentField.schema())) {
        Schema nonNullSchema = subSchemas.get(0).getType() == Schema.Type.NULL ? subSchemas.get(1) : subSchemas.get(0);
        // If the nested schema is not the same, fail the validation
        if (nonNullSchema.getType() != targetField.schema().getType()) {
          return returnTrueAndLogError(
              String.format(
                  "Field %s: Type mismatch %s vs %s",
                  formatFieldName(currentField.name()),
                  nonNullSchema.getType(),
                  targetField.schema().getType()));
        }
        // If the nested schema is the same, check the value
        return isNonDefaultValue(
            object,
            AvroCompatibilityHelper.createSchemaField(currentField.name(), nonNullSchema, "", null),
            targetField);
      }

      // Fail the validation since the target field is not a union and the current field is not a nullable union pair
      return returnTrueAndLogError(
          String.format(
              "Field %s: Type mismatch %s vs %s",
              formatFieldName(currentField.name()),
              currentField.schema().getType(),
              targetField.schema().getType()));
    }

    // Expect the target field to be a union
    Map<String, Schema> targetSchemaMap =
        targetField.schema().getTypes().stream().collect(Collectors.toMap(s -> s.getName(), s -> s));
    for (Schema subSchema: currentField.schema().getTypes()) {
      try {
        object = castValueToSchema(object, subSchema);

        if (targetSchemaMap.containsKey(subSchema.getName())) {
          return isNonDefaultValue(
              object,
              AvroCompatibilityHelper.createSchemaField(currentField.name(), subSchema, "", null),
              AvroCompatibilityHelper
                  .createSchemaField(currentField.name(), targetSchemaMap.get(subSchema.getName()), "", null));
        }

        return isNonDefaultValue(
            object,
            AvroCompatibilityHelper.createSchemaField(currentField.name(), subSchema, "", null),
            null);
      } catch (IllegalArgumentException e) {
        // Ignore and continue checking other subtypes
      }
    }
    return returnTrueAndLogError(
        String.format(
            "Field %s: Cannot find the match schema for value %s from schema union",
            formatFieldName(currentField.name()),
            object));
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

  private static Object castValueToSchema(Object value, Schema schema) {
    if (value == null)
      return null;

    switch (schema.getType()) {
      case STRING:
        return value.toString();
      case INT:
        return (value instanceof Number) ? ((Number) value).intValue() : throwTypeException(value, schema);
      case LONG:
        return (value instanceof Number) ? ((Number) value).longValue() : throwTypeException(value, schema);
      case FLOAT:
        return (value instanceof Number) ? ((Number) value).floatValue() : throwTypeException(value, schema);
      case DOUBLE:
        return (value instanceof Number) ? ((Number) value).doubleValue() : throwTypeException(value, schema);
      case BOOLEAN:
        return (value instanceof Boolean) ? value : throwTypeException(value, schema);
      case ENUM:
        return schema.hasEnumSymbol(value.toString()) ? value.toString() : throwTypeException(value, schema);
      case FIXED:
        return (value instanceof byte[])
            ? AvroCompatibilityHelper.newFixed(schema, (byte[]) value)
            : throwTypeException(value, schema);
      case BYTES:
        return (value instanceof byte[]) ? value : throwTypeException(value, schema);
      case RECORD:
        return (value instanceof GenericRecord) ? value : throwTypeException(value, schema);
      case ARRAY:
        return (value instanceof List) ? value : throwTypeException(value, schema);
      case MAP:
        return (value instanceof Map) ? value : throwTypeException(value, schema);
      default:
        throw new IllegalArgumentException("Unsupported schema type: " + schema.getType());
    }
  }

  private static Object throwTypeException(Object value, Schema schema) {
    throw new IllegalArgumentException("Value " + value + " does not match schema type: " + schema.getType());
  }

  public String getErrorMessage() {
    return errorMessage.get();
  }
}
