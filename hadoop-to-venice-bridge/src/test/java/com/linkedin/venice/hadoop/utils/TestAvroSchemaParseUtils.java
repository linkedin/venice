package com.linkedin.venice.hadoop.utils;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAvroSchemaParseUtils {

    private static final String SCHEMA_FAIL_EXTENDED_VALIDATION = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"name_1\",\n" +
            "  \"fields\" : [\n" +
            "    { \"name\" : \"name_2\", \"type\" : \"long\" },\n" +
            "    {\n" +
            "      \"name\" : \"name_3\",\n" +
            "      \"type\" : {\n" +
            "        \"type\" : \"record\",\n" +
            "        \"name\" : \"name_4\",\n" +
            "        \"fields\" : [\n" +
            "          {\n" +
            "            \"name\" : \"name_5\",\n" +
            "            \"type\" : [\"null\", {\n" +
            "              \"type\" : \"array\",\n" +
            "              \"items\" : {\n" +
            "                \"type\" : \"record\",\n" +
            "                \"name\" : \"name_6\",\n" +
            "                \"fields\" : [\n" +
            "                  { \"name\" : \"name_7\", \"type\" : \"string\" },\n" +
            "                  { \"name\" : \"name_8\", \"type\" : \"int\" },\n" +
            "                  { \"name\" : \"name_9\", \"type\" : { \"type\" : \"array\", \"items\" : \"float\" } }\n" +
            "                ]\n" +
            "              }\n" +
            "            }],\n" +
            "            \"default\" : \"null\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    private static final String SCHEMA_PASS_EXTENDED_VALIDATION = "{\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"Type1\",\n" +
            "    \"fields\": [\n" +
            "        {\n" +
            "            \"name\": \"something\",\n" +
            "            \"type\": \"string\"\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    @Test
    public void testParseSchemaFailExtendedValidation() {
        Exception expectedException = null;
        try {
            AvroSchemaParseUtils.parseSchemaFromJSONWithExtendedValidation(SCHEMA_FAIL_EXTENDED_VALIDATION);
        } catch (Exception e) {
            expectedException = e;
        }
        Assert.assertNotNull(expectedException, "Expect to fail on parsing this schema with extended schema validation");
        Schema schema = AvroSchemaParseUtils.parseSchemaFromJSONWithNoExtendedValidation(SCHEMA_FAIL_EXTENDED_VALIDATION);
        Assert.assertNotNull(schema, "Expected to have a successfully parsed schema");
    }

    @Test(expectedExceptions = VeniceException.class)
    public void testParseSchemaFailWhenExtendedValidationEnabled() {
        final boolean extendedSchemaValidityCheckEnabled = true;
        AvroSchemaParseUtils.parseSchemaFromJSON(SCHEMA_FAIL_EXTENDED_VALIDATION, extendedSchemaValidityCheckEnabled);
    }

    @Test
    public void testParseSchemaSuccessfullyWhenExtendedValidationNotEnabled() {
        final boolean extendedSchemaValidityCheckEnabled = false;
        AvroSchemaParseUtils.parseSchemaFromJSON(SCHEMA_FAIL_EXTENDED_VALIDATION, extendedSchemaValidityCheckEnabled);
    }

    @Test
    public void testParseSchemaSuccessfullyWhenExtendedValidationEnabled() {
        final boolean extendedSchemaValidityCheckEnabled = true;
        AvroSchemaParseUtils.parseSchemaFromJSON(SCHEMA_PASS_EXTENDED_VALIDATION, extendedSchemaValidityCheckEnabled);
    }
}
