package com.linkedin.venice.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AvroSchemaParseUtils {
  private static final Logger LOGGER = LogManager.getLogger(AvroSchemaParseUtils.class);

  private AvroSchemaParseUtils() {
    // Util class
  }

  public static Schema parseSchemaFromJSON(String jsonSchema, boolean extendedSchemaValidityCheckEnabled) {
    Schema schema;
    try {
      schema = parseSchemaFromJSONStrictValidation(jsonSchema);
    } catch (Exception e) {
      if (extendedSchemaValidityCheckEnabled) {
        throw new VeniceException(e);
      }
      LOGGER.warn(
          String.format(
              "Failed to parse schema %s failed using %s. Fall back to use the Avro schema parser",
              jsonSchema,
              AvroCompatibilityHelper.class),
          e);
      return parseSchemaFromJSONLooseValidation(jsonSchema);
    }
    return schema;
  }

  public static Schema parseSchemaFromJSONStrictValidation(String jsonSchema) {
    return AvroCompatibilityHelper.parse(jsonSchema, SchemaParseConfiguration.STRICT, null).getMainSchema();
  }

  /**
   * Parses with {@link SchemaParseConfiguration#LOOSE_NUMERICS}, which differs from STRICT only by
   * disabling {@code validateNumericDefaultValueTypes}. Numeric defaults whose JSON tier doesn't
   * match the field's declared numeric type (e.g. {@code "default": 0} for a {@code float} field)
   * are accepted at parse time. All other strict checks (name validation,
   * default-actually-matches-type, dangling-content) still apply.
   *
   * Note: Avro retains the original {@code JsonNode} as the default — it does NOT coerce the in-memory
   * default to the declared type. Use {@link #coerceNumericDefaultsToFieldType(String)} if you need a
   * string form that subsequently passes STRICT.
   */
  public static Schema parseSchemaFromJSONLooseNumericValidation(String jsonSchema) {
    return AvroCompatibilityHelper.parse(jsonSchema, SchemaParseConfiguration.LOOSE_NUMERICS, null).getMainSchema();
  }

  public static Schema parseSchemaFromJSONLooseValidation(String jsonSchema) {
    return AvroCompatibilityHelper.parse(jsonSchema, SchemaParseConfiguration.LOOSE, null).getMainSchema();
  }

  /**
   * Walks the schema JSON and coerces numeric default values to the declared primitive numeric field
   * type. For example {@code {"name":"score","type":"float","default":0}} becomes
   * {@code {"name":"score","type":"float","default":0.0}} — making the schema strict-parse-clean
   * without changing its semantic meaning (Avro fingerprint / parsing canonical form is unaffected
   * since defaults are not part of the canonical form).
   *
   * Only acts on field specs whose {@code type} is a textual primitive numeric name and whose
   * {@code default} is a numeric JSON value of a different tier. Does not touch unions, complex
   * types, non-numeric defaults, or fields whose default already matches the declared type.
   *
   * Returns the input unchanged if no coercion was needed.
   *
   * Intended for use during store migration to rewrite legacy schemas registered before
   * {@code validateNumericDefaultValueTypes} was enforced.
   */
  public static String coerceNumericDefaultsToFieldType(String jsonSchema) {
    try {
      ObjectMapper mapper = ObjectMapperFactory.getInstance();
      JsonNode root = mapper.readTree(jsonSchema);
      if (coerceInPlace(root)) {
        return mapper.writeValueAsString(root);
      }
      return jsonSchema;
    } catch (IOException e) {
      throw new VeniceException("Failed to parse JSON for numeric default coercion: " + jsonSchema, e);
    }
  }

  private static boolean coerceInPlace(JsonNode node) {
    boolean changed = false;
    if (node.isArray()) {
      for (JsonNode child: node) {
        if (coerceInPlace(child)) {
          changed = true;
        }
      }
      return changed;
    }
    if (!node.isObject()) {
      return false;
    }
    ObjectNode obj = (ObjectNode) node;
    JsonNode typeNode = obj.get("type");
    JsonNode defaultNode = obj.get("default");
    if (typeNode != null && typeNode.isTextual() && defaultNode != null && defaultNode.isNumber()) {
      JsonNode coerced = coerceNumber(defaultNode, typeNode.asText());
      if (coerced != null) {
        obj.set("default", coerced);
        changed = true;
      }
    }
    for (Iterator<String> it = obj.fieldNames(); it.hasNext();) {
      if (coerceInPlace(obj.get(it.next()))) {
        changed = true;
      }
    }
    return changed;
  }

  private static JsonNode coerceNumber(JsonNode value, String declaredType) {
    switch (declaredType) {
      case "float":
        if (value.isFloat() || value.isDouble()) {
          return null;
        }
        return new FloatNode(value.floatValue());
      case "double":
        if (value.isDouble()) {
          return null;
        }
        return new DoubleNode(value.doubleValue());
      case "int":
        if (value.isInt()) {
          return null;
        }
        return new IntNode(value.intValue());
      case "long":
        if (value.isLong()) {
          return null;
        }
        return new LongNode(value.longValue());
      default:
        return null;
    }
  }
}
