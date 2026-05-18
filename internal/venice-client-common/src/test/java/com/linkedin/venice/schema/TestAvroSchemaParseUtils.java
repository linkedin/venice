package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Branch-coverage tests for {@link AvroSchemaParseUtils}.
 *
 * Lives in {@code venice-client-common} (alongside the class under test) so coverage tooling on
 * this module sees the exercised branches. There is a similar test file in {@code venice-push-job}
 * that covers a few overlapping cases but doesn't count toward this module's diff coverage.
 */
public class TestAvroSchemaParseUtils {
  // STRICT-clean record schema.
  private static final String CLEAN_RECORD_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Clean\",\"fields\":[" + "{\"name\":\"v\",\"type\":\"string\"}]}";

  // Legacy: int default on a float field — fails STRICT (numeric tier), accepted by LOOSE_NUMERICS.
  private static final String LEGACY_INT_ON_FLOAT = "{\"type\":\"record\",\"name\":\"Scores\",\"fields\":["
      + "{\"name\":\"score\",\"type\":\"float\",\"default\":0}]}";

  // Union default whose JSON tier matches the second branch, not the first — STRICT failure outside
  // the numeric-default tier. LOOSE_NUMERICS must still reject this.
  private static final String UNION_DEFAULT_NOT_FIRST_BRANCH = "{\"type\":\"record\",\"name\":\"BadUnion\",\"fields\":["
      + "{\"name\":\"f\",\"type\":[\"int\",\"null\"],\"default\":null}]}";

  // Schema mixing all four numeric tiers with mismatched defaults — exercises every case in the
  // coerceNumber switch.
  private static final String ALL_NUMERIC_TIERS = "{\"type\":\"record\",\"name\":\"AllTiers\",\"fields\":["
      + "{\"name\":\"f\",\"type\":\"float\",\"default\":0}," + "{\"name\":\"d\",\"type\":\"double\",\"default\":1},"
      + "{\"name\":\"l\",\"type\":\"long\",\"default\":2.0}," + "{\"name\":\"i\",\"type\":\"int\",\"default\":3.0}]}";

  // ----- parseSchemaFromJSONStrictValidation ----------------------------------------------------

  @Test
  public void strictAcceptsCleanSchema() {
    Schema s = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(CLEAN_RECORD_SCHEMA);
    Assert.assertEquals(s.getType(), Schema.Type.RECORD);
  }

  @Test
  public void strictRejectsLegacyIntOnFloat() {
    Assert.assertThrows(
        Exception.class,
        () -> AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(LEGACY_INT_ON_FLOAT));
  }

  // ----- parseSchemaFromJSONLooseNumericValidation ----------------------------------------------

  @Test
  public void looseNumericAcceptsLegacyIntOnFloat() {
    Schema s = AvroSchemaParseUtils.parseSchemaFromJSONLooseNumericValidation(LEGACY_INT_ON_FLOAT);
    Assert.assertNotNull(s);
  }

  @Test
  public void looseNumericStillRejectsNonNumericStrictViolations() {
    // The union-default-not-first-branch case is outside the numeric-default tier this preset
    // relaxes — must still throw.
    Assert.assertThrows(
        Exception.class,
        () -> AvroSchemaParseUtils.parseSchemaFromJSONLooseNumericValidation(UNION_DEFAULT_NOT_FIRST_BRANCH));
  }

  // ----- parseSchemaFromJSONLooseValidation -----------------------------------------------------

  @Test
  public void looseAcceptsLegacyIntOnFloat() {
    Schema s = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(LEGACY_INT_ON_FLOAT);
    Assert.assertNotNull(s);
  }

  // ----- parseSchemaFromJSON(str, extendedSchemaValidityCheckEnabled) ---------------------------

  @Test
  public void parseSchemaFromJSONStrictPassesThroughForCleanSchema() {
    // Clean schema → strict parser succeeds, no fallback triggered.
    Schema s = AvroSchemaParseUtils.parseSchemaFromJSON(CLEAN_RECORD_SCHEMA, true);
    Assert.assertEquals(s.getType(), Schema.Type.RECORD);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void parseSchemaFromJSONThrowsWhenExtendedValidationOnAndStrictFails() {
    AvroSchemaParseUtils.parseSchemaFromJSON(LEGACY_INT_ON_FLOAT, true);
  }

  @Test
  public void parseSchemaFromJSONFallsBackToLooseWhenExtendedValidationOff() {
    // Strict fails, but extendedSchemaValidityCheckEnabled=false so we fall back to LOOSE and
    // return a parsed schema rather than throwing.
    Schema s = AvroSchemaParseUtils.parseSchemaFromJSON(LEGACY_INT_ON_FLOAT, false);
    Assert.assertNotNull(s);
  }

  // ----- coerceNumericDefaultsToFieldType -------------------------------------------------------

  @Test
  public void coerceRewritesIntDefaultOnFloatField() {
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(LEGACY_INT_ON_FLOAT);
    Assert.assertNotEquals(coerced, LEGACY_INT_ON_FLOAT, "Walker must rewrite the legacy default");
    // Round-trip: the rewritten output must pass STRICT.
    AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(coerced);
  }

  @Test
  public void coerceHandlesAllFourNumericTiers() {
    // Walks each branch of coerceNumber's switch (float / double / int / long with mismatched JSON tier).
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(ALL_NUMERIC_TIERS);
    AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(coerced);
  }

  @Test
  public void coerceReturnsInputUnchangedWhenCleanSchema() {
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(CLEAN_RECORD_SCHEMA);
    Assert.assertEquals(coerced, CLEAN_RECORD_SCHEMA, "Clean schema must short-circuit the walker");
  }

  @Test
  public void coerceLeavesNonNumericStrictViolationsAlone() {
    // The walker is a numeric-default fixer only. A non-numeric STRICT violation must still trip
    // strict parse on the walker's output.
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(UNION_DEFAULT_NOT_FIRST_BRANCH);
    Assert.assertThrows(Exception.class, () -> AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(coerced));
  }

  @Test
  public void coerceRecursesIntoNestedRecord() {
    // Nested record with a legacy numeric default inside — exercises the recursion path through
    // a non-array, non-primitive {@code type} value.
    String nested = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
        + "{\"name\":\"score\",\"type\":\"float\",\"default\":0}]}}]}";
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(nested);
    Assert.assertNotEquals(coerced, nested);
    AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(coerced);
  }

  @Test
  public void coerceLeavesNonNumericDefaultsUnchanged() {
    // String-typed field with a string default — not a coercion target.
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(CLEAN_RECORD_SCHEMA);
    Assert.assertEquals(coerced, CLEAN_RECORD_SCHEMA);
  }

  @Test
  public void coerceLeavesNonTextualTypeUnchanged() {
    // When the {@code type} is itself an object (e.g. a nested record definition) rather than a
    // textual primitive name, the field-spec check skips and only recursion fires.
    String schema = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}";
    String coerced = AvroSchemaParseUtils.coerceNumericDefaultsToFieldType(schema);
    Assert.assertEquals(coerced, schema);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void coerceWrapsInvalidJsonAsVeniceException() {
    AvroSchemaParseUtils.coerceNumericDefaultsToFieldType("{not valid json");
  }
}
