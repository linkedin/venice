package com.linkedin.venice.hadoop.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAvroSchemaParseUtils {
  private static final String SCHEMA_FAIL_EXTENDED_VALIDATION = "{\n" + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"name_1\",\n" + "  \"fields\" : [\n" + "    { \"name\" : \"name_2\", \"type\" : \"long\" },\n"
      + "    {\n" + "      \"name\" : \"name_3\",\n" + "      \"type\" : {\n" + "        \"type\" : \"record\",\n"
      + "        \"name\" : \"name_4\",\n" + "        \"fields\" : [\n" + "          {\n"
      + "            \"name\" : \"name_5\",\n" + "            \"type\" : [\"null\", {\n"
      + "              \"type\" : \"array\",\n" + "              \"items\" : {\n"
      + "                \"type\" : \"record\",\n" + "                \"name\" : \"name_6\",\n"
      + "                \"fields\" : [\n" + "                  { \"name\" : \"name_7\", \"type\" : \"string\" },\n"
      + "                  { \"name\" : \"name_8\", \"type\" : \"int\" },\n"
      + "                  { \"name\" : \"name_9\", \"type\" : { \"type\" : \"array\", \"items\" : \"float\" } }\n"
      + "                ]\n" + "              }\n" + "            }],\n" + "            \"default\" : \"null\"\n"
      + "          }\n" + "        ]\n" + "      }\n" + "    }\n" + "  ]\n" + "}";

  private static final String SCHEMA_PASS_EXTENDED_VALIDATION = "{\n" + "    \"type\": \"record\",\n"
      + "    \"name\": \"Type1\",\n" + "    \"fields\": [\n" + "        {\n" + "            \"name\": \"something\",\n"
      + "            \"type\": \"string\"\n" + "        }\n" + "    ]\n" + "}";

  @Test
  public void testParseSchemaFailExtendedValidation() {
    Exception expectedException = null;
    try {
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(SCHEMA_FAIL_EXTENDED_VALIDATION);
    } catch (Exception e) {
      expectedException = e;
    }
    Assert.assertNotNull(expectedException, "Expect to fail on parsing this schema with extended schema validation");
    Schema schema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(SCHEMA_FAIL_EXTENDED_VALIDATION);
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

  // Legacy-style schema with an int default on a float field — fails STRICT (numeric tier check),
  // accepted by LOOSE_NUMERICS, and rewritten by coerceNumericDefaultsToFieldType.
  private static final String SCHEMA_NUMERIC_DEFAULT_MISMATCH =
      "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"Scores\",\n" + "  \"fields\": [\n"
          + "    { \"name\": \"score\", \"type\": \"float\", \"default\": 0 }\n" + "  ]\n" + "}";

  // Union with default whose declared type matches the second branch, not the first — a STRICT
  // failure that's outside the numeric-default tier. Confirms LOOSE_NUMERICS only opens the one door.
  private static final String SCHEMA_UNION_DEFAULT_NOT_FIRST_BRANCH =
      "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"BadUnionDefault\",\n" + "  \"fields\": [\n"
          + "    { \"name\": \"f\", \"type\": [\"int\", \"null\"], \"default\": null }\n" + "  ]\n" + "}";

  @Test
  public void testLooseNumericAcceptsIntDefaultOnFloatField() {
    // STRICT rejects this — the historical migration foot-gun.
    Assert.assertThrows(
        Exception.class,
        () -> AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(SCHEMA_NUMERIC_DEFAULT_MISMATCH));

    // LOOSE_NUMERICS accepts it.
    Schema parsed = AvroSchemaParseUtils.parseSchemaFromJSONLooseNumericValidation(SCHEMA_NUMERIC_DEFAULT_MISMATCH);
    Assert.assertNotNull(parsed);
  }

  @Test
  public void testLooseNumericRejectsNonNumericStrictViolation() {
    // The union-default-not-first-branch case is a STRICT failure that LOOSE_NUMERICS preserves —
    // it's outside the numeric-default tier this preset relaxes.
    Assert.assertThrows(
        Exception.class,
        () -> AvroSchemaParseUtils.parseSchemaFromJSONLooseNumericValidation(SCHEMA_UNION_DEFAULT_NOT_FIRST_BRANCH));
  }

  @Test
  public void testCoerceNumericDefaultsRewritesIntDefaultOnFloatField() {
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(SCHEMA_NUMERIC_DEFAULT_MISMATCH);
    Assert.assertNotEquals(coerced, SCHEMA_NUMERIC_DEFAULT_MISMATCH, "Legacy schema must be rewritten");
    // Output must be strict-parse-clean — that's the whole point.
    Schema strictParsed = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(coerced);
    Assert.assertNotNull(strictParsed);
  }

  @Test
  public void testCoerceNumericDefaultsIsNoOpForCleanSchema() {
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(SCHEMA_PASS_EXTENDED_VALIDATION);
    Assert.assertSame(coerced, SCHEMA_PASS_EXTENDED_VALIDATION, "Clean schema must be returned by identity");
  }

  @Test
  public void testCoerceNumericDefaultsDoesNotFixNonNumericIssues() {
    // The walker only fixes numeric default mismatches. Other STRICT violations must remain.
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(SCHEMA_UNION_DEFAULT_NOT_FIRST_BRANCH);
    Assert.assertThrows(Exception.class, () -> AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(coerced));
  }

  @Test
  public void testCoerceNumericDefaultsHandlesAllNumericTiers() {
    String schema = "{\"type\":\"record\",\"name\":\"AllTiers\",\"fields\":["
        + "{\"name\":\"f\",\"type\":\"float\",\"default\":0}," + "{\"name\":\"d\",\"type\":\"double\",\"default\":1},"
        + "{\"name\":\"l\",\"type\":\"long\",\"default\":2.0}," + "{\"name\":\"i\",\"type\":\"int\",\"default\":3.0}"
        + "]}";
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(schema);
    AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(coerced);
  }

  @Test
  public void testParseSchemaStrWithDifferentConfigurations() {
    String schemaStr = "{" + "    \"doc\": \"Value in the store\"," + "    \"fields\": [" + "      {\n"
        + "         \"default\": null," + "         \"name\": \"fieldName\"," + "         \"type\": [" + // To pass the
                                                                                                         // STRICT mode,
                                                                                                         // order of
                                                                                                         // these 2
                                                                                                         // types needs
                                                                                                         // to be
                                                                                                         // swapped.
        "           \"float\"," + "           \"null\"" + "        ]" + "      }" + "    ],"
        + "    \"name\": \"someName\"," + "    \"type\": \"record\"" + "}";

    // Can parse the schema in the LOOSE mode
    Schema schema = AvroSchemaParseUtils.parseSchemaFromJSON(schemaStr, false);
    Assert.assertNotNull(schema);

    // Cannot parse the schema in the STRICT mode
    Assert.assertThrows(VeniceException.class, () -> AvroSchemaParseUtils.parseSchemaFromJSON(schemaStr, true));
  }
}
