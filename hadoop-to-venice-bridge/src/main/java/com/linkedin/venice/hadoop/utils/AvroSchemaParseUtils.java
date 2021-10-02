package com.linkedin.venice.hadoop.utils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

public class AvroSchemaParseUtils {
    private static final Logger LOGGER = Logger.getLogger(AvroSchemaParseUtils.class);

    private AvroSchemaParseUtils() {
        // Util class
    }

    public static Schema parseSchemaFromJSON(String jsonSchema, boolean extendedSchemaValidityCheckEnabled) {
        Schema schema;
        try {
            schema = parseSchemaFromJSONWithExtendedValidation(jsonSchema);
        } catch (Exception e) {
            if (extendedSchemaValidityCheckEnabled) {
                throw new VeniceException(e);
            }
            LOGGER.warn(String.format("Failed to parse schema %s failed using %s. Fall back to use the Avro schema parser",
                    jsonSchema, AvroCompatibilityHelper.class), e);
            return parseSchemaFromJSONWithNoExtendedValidation(jsonSchema);
        }
        return schema;
    }

    public static Schema parseSchemaFromJSONWithExtendedValidation(String jsonSchema) {
        return AvroCompatibilityHelper.parse(jsonSchema, SchemaParseConfiguration.STRICT, null).getMainSchema();
    }

    public static Schema parseSchemaFromJSONWithNoExtendedValidation(String jsonSchema) {
        return AvroCompatibilityHelper.parse(jsonSchema, SchemaParseConfiguration.LOOSE, null).getMainSchema();
    }
}
