package com.linkedin.venice.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.venice.exceptions.VeniceException;
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

  public static Schema parseSchemaFromJSONLooseValidation(String jsonSchema) {
    return AvroCompatibilityHelper.parse(jsonSchema, SchemaParseConfiguration.LOOSE, null).getMainSchema();
  }
}
