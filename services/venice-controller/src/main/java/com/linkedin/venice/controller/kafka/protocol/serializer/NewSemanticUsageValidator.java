package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.Pair;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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

  private static final BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> SEMANTIC_VALIDATOR =
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
        return checkSchemaSpecificCases(object, currentField, targetField);
      };

  public BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> getSemanticValidator() {
    return SEMANTIC_VALIDATOR;
  }

  private static boolean hasSchemaTypeMismatch(Schema.Field currentField, Schema.Field targetField) {
    if (currentField.schema().getType() != targetField.schema().getType()) {
      errorMessage.set(
          String.format(
              "Field " + formatFieldName(currentField.name()) + ": Type mismatch %s vs %s",
              currentField.schema().getType(),
              targetField.schema().getType()));
      return true;
    }
    return false;
  }

  private static boolean checkSchemaSpecificCases(Object object, Schema.Field currentField, Schema.Field targetField) {
    switch (currentField.schema().getType()) {
      case UNION:
        for (Schema subSchema: currentField.schema().getTypes()) {
          if (subSchema.getType() == Schema.Type.NULL) {
            continue;
          }

          try {
            object = castValueToSchema(object, subSchema);
            return isNonDefaultValue(object, new Schema.Field(currentField.name(), subSchema, "", null));
          } catch (IllegalArgumentException e) {
            // Ignore and continue checking other subtypes
          }
        }
        errorMessage.set("Field: " + formatFieldName(currentField.name()) + " has an incompatible union type");
        return true;
      case ENUM:
        return isNewEnumValue(object, currentField, targetField);
      case FIXED:
        if (currentField.schema().getFixedSize() != targetField.schema().getFixedSize()) {
          errorMessage.set("Field: " + formatFieldName(currentField.name()) + " has different fixed size");
          return true;
        }
        return false;
      default:
        return isNonDefaultValue(object, currentField);
    }
  }

  private static String formatFieldName(String fieldName) {
    return fieldName.replace("_", ".");
  }

  private static boolean isNewEnumValue(Object object, Schema.Field currentField, Schema.Field targetField) {
    List<String> currentSymbols = currentField.schema().getEnumSymbols();
    List<String> targetSymbols = targetField.schema().getEnumSymbols();
    if (targetSymbols.stream().anyMatch(symbol -> !currentSymbols.contains(symbol) && symbol.equals(object))) {
      errorMessage.set("Field: " + formatFieldName(currentField.name()) + " contains new enum value: " + object);
      return true;
    }
    return false;
  }

  private static boolean isNonDefaultValue(Object value, Schema.Field field) {
    if (value == null)
      return false;

    Object fieldDefaultValue = AvroSchemaUtils.getFieldDefault(field);
    Object defaultValue = getDefaultForType(field.schema().getType());

    return compareObjectToDefaultValue(value, defaultValue, fieldDefaultValue, field.name());
  }

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

  private static boolean compareObjectToDefaultValue(
      Object object,
      Object defaultValue,
      Object fieldDefaultValue,
      String fieldName) {
    if (!Objects.equals(object, defaultValue) && !Objects.equals(object, fieldDefaultValue)) {
      errorMessage.set(
          "Field: " + formatFieldName(fieldName) + " contains non-default value. Actual value: " + object
              + ". Default value: " + fieldDefaultValue + " or " + defaultValue);
      return true;
    }
    return false;
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
            ? new GenericData.Fixed(schema, (byte[]) value)
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
