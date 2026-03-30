package com.linkedin.venice.schema;

import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V4_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V5_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V6_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.USER_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.USER_WITH_DEFAULT_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.USER_WITH_NESTED_RECORD_AND_DEFAULT_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.USER_WITH_NESTED_RECORD_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAvroSupersetSchemaUtils {
  @Test
  public void testGenerateSupersetSchemaFromValueSchemasWithTwoSchemas() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," + "\"fields\":" + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"}" + " ]" + "}";

    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," + "\"fields\":" + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"company\",\"type\":\"string\", \"default\" : \"linkedin\"}" + " ]" + "}";

    SchemaEntry schemaEntry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry schemaEntry2 = new SchemaEntry(2, schemaStr2);

    SchemaEntry supersetSchemaEntry =
        AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(Arrays.asList(schemaEntry1, schemaEntry2));

    final Schema expectedSupersetSchema =
        AvroSupersetSchemaUtils.generateSupersetSchema(schemaEntry1.getSchema(), schemaEntry2.getSchema());
    Assert.assertTrue(
        AvroSchemaUtils.compareSchemaIgnoreFieldOrder(expectedSupersetSchema, supersetSchemaEntry.getSchema()));
    Assert.assertEquals(supersetSchemaEntry.getId(), 2);

    supersetSchemaEntry =
        AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(Arrays.asList(schemaEntry2, schemaEntry1) // Order
                                                                                                            // should
                                                                                                            // not
                                                                                                            // matter.
        );
    Assert.assertTrue(
        AvroSchemaUtils.compareSchemaIgnoreFieldOrder(expectedSupersetSchema, supersetSchemaEntry.getSchema()));
    Assert.assertEquals(supersetSchemaEntry.getId(), 2);

    // Test the case where generated superset schema entry should have schema ID 1 instead of 2.
    schemaEntry1 = new SchemaEntry(2, schemaStr1);
    schemaEntry2 = new SchemaEntry(1, schemaStr2);

    supersetSchemaEntry =
        AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(Arrays.asList(schemaEntry2, schemaEntry1) // Order
                                                                                                            // should
                                                                                                            // not
                                                                                                            // matter.
        );
    Assert.assertTrue(
        AvroSchemaUtils.compareSchemaIgnoreFieldOrder(expectedSupersetSchema, supersetSchemaEntry.getSchema()));
    Assert.assertEquals(supersetSchemaEntry.getId(), 1);
  }

  @Test
  public void testGenerateSupersetSchemaFromThreeValueSchemas() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," + "\"fields\":" + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"}" + " ]" + "}";

    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," + "\"fields\":" + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"company\",\"type\":\"string\", \"default\" : \"linkedin\"}"
        // New field compared to schemaStr1
        + " ]" + "}";

    String schemaStr3 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," + "\"fields\":" + " ["
        + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"age\",\"type\":\"int\", \"default\" : 29}" // Another new field compared to schemaStr1
        + " ]" + "}";

    String schemaStr4 = "{\"type\":\"record\",\"name\":\"KeyRecord\"," // This schema contains all fields.
        + "\"fields\":" + " [" + "   {\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},"
        + "   {\"name\":\"company\",\"type\":\"string\", \"default\" : \"linkedin\"},"
        + "   {\"name\":\"age\",\"type\":\"int\", \"default\" : 29}" + " ]" + "}";

    final SchemaEntry schemaEntry1 = new SchemaEntry(1, schemaStr1);
    final SchemaEntry schemaEntry2 = new SchemaEntry(2, schemaStr2);
    final SchemaEntry schemaEntry3 = new SchemaEntry(3, schemaStr3);
    final SchemaEntry schemaEntry4 = new SchemaEntry(4, schemaStr4);
    final Schema expectedSupersetSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr4);

    // Case 1: generate a superset new schema entry with a new ID.
    SchemaEntry supersetSchemaEntry = AvroSchemaUtils
        .generateSupersetSchemaFromAllValueSchemas(Arrays.asList(schemaEntry1, schemaEntry2, schemaEntry3));
    Assert.assertTrue(
        AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
    Assert.assertEquals(supersetSchemaEntry.getId(), 4);

    // Case 2: generate a superset schema entry that is the same as schemaEntry4.
    supersetSchemaEntry = AvroSchemaUtils
        .generateSupersetSchemaFromAllValueSchemas(Arrays.asList(schemaEntry1, schemaEntry2, schemaEntry4));
    Assert.assertTrue(
        AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
    Assert.assertEquals(supersetSchemaEntry.getId(), 4);

    // Case 3: any schema + schemaEntry4 can generate a schema entry that is the same as schemaEntry4.
    for (SchemaEntry schemaEntry: Arrays.asList(schemaEntry1, schemaEntry2, schemaEntry3)) {
      supersetSchemaEntry =
          AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(Arrays.asList(schemaEntry, schemaEntry4));
      Assert.assertTrue(
          AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
      Assert.assertEquals(supersetSchemaEntry.getId(), 4);

      supersetSchemaEntry =
          AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(Arrays.asList(schemaEntry4, schemaEntry)
          // Order should not matter.
          );
      Assert.assertTrue(
          AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetSchemaEntry.getSchema(), expectedSupersetSchema));
      Assert.assertEquals(supersetSchemaEntry.getId(), 4);
    }
  }

  @Test
  public void testSupersetSchemaDefaultCompatibility() {
    String valueSchemaStr1 =
        "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
            + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  "
            + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  "
            + "       { \"name\": \"weight\", \"type\": \"float\", \"default\": 0.0},  "
            + "       { \"name\": \"age\", \"type\": \"float\", \"default\": 0.0 }" + "  ] " + " } ";

    String valueSchemaStr2 =
        "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
            + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  "
            + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  "
            + "       { \"name\": \"address\", \"type\": \"string\", \"default\": \"italy\" }" + "  ] " + " } ";

    Schema newValueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr1);
    Schema existingValueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr2);
    Schema newSuperSetSchema = AvroSupersetSchemaUtils.generateSupersetSchema(existingValueSchema, newValueSchema);
    Assert.assertTrue(
        new SchemaEntry(1, valueSchemaStr2)
            .isNewSchemaCompatible(new SchemaEntry(2, newSuperSetSchema), DirectionalSchemaCompatibilityType.FULL));
  }

  @Test
  public void testStringVsAvroString() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\": {\"type\" : \"string\", \"avro.java.string\" : \"String\"},\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);

    Assert.assertNotEquals(s1, s2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSupersetSchema(s2, s1);
    Assert.assertNotNull(s3);
    Assert.assertNotNull(
        AvroCompatibilityHelper.getSchemaPropAsJsonString(s3.getField("name").schema(), "avro.java.string"));
  }

  @Test
  public void testWithDifferentDocField() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Assert.assertNotNull(s3);
  }

  @Test
  public void testSchemaMerge() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"business\",\"type\":\"string\"}]}";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));
    Schema s3 = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Assert.assertNotNull(s3);
  }

  @Test
  public void testSchemaMergeEnumSymbols() {
    // Superset of two schemas whose enum field has diverged symbols should contain all symbols
    // from both, with existing-schema symbols first (order preserved) and new symbols appended.
    String existing = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"status\",\"type\":"
        + "{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"A\",\"B\",\"C\"]}}]}";
    String newer = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"status\",\"type\":"
        + "{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"A\",\"B\",\"D\"]}}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(existing);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(newer);
    Schema superset = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);

    List<String> symbols = superset.getField("status").schema().getEnumSymbols();
    // All four symbols present; existing order preserved; new symbol appended
    Assert.assertEquals(symbols, Arrays.asList("A", "B", "C", "D"));
  }

  /**
   * newSchema is already a symbol superset of existingSchema (only adds "XL"). Properties are
   * still merged: existingSchema-only props are preserved, shared props use newSchema's value.
   */
  @Test
  public void testSchemaMergeEnumSymbolsIdentical() {
    // existingSchema ["S","M","L"] has "old-only" prop; newSchema ["S","M","L","XL"] has "extra-prop".
    // "shared" exists in both — newSchema wins.
    String existing = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"size\",\"type\":"
        + "{\"type\":\"enum\",\"name\":\"Size\",\"symbols\":[\"S\",\"M\",\"L\"],"
        + "\"old-only\":\"preserved\",\"shared\":\"old-val\"}}]}";
    String newer = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"size\",\"type\":"
        + "{\"type\":\"enum\",\"name\":\"Size\",\"symbols\":[\"S\",\"M\",\"L\",\"XL\"],"
        + "\"default\":\"S\",\"shared\":\"new-val\"}}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(existing);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(newer);
    Schema superset = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Schema supersetEnum = superset.getField("size").schema();

    Assert.assertEquals(supersetEnum.getEnumSymbols(), Arrays.asList("S", "M", "L", "XL"));
    // existingSchema-only prop is preserved even though newSchema is the symbol superset
    Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(supersetEnum, "old-only"), "\"preserved\"");
    // shared prop: newSchema wins
    Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(supersetEnum, "shared"), "\"new-val\"");
    Assert.assertEquals(supersetEnum.getEnumDefault(), "S");
  }

  /**
   * Slow-path test: symbols change (both added and dropped between versions) AND properties
   * differ. A new enum schema is constructed with the union of all symbols and the merged
   * property set.
   */
  @Test
  public void testSchemaMergeEnumChangedSymbolsAndProps() {
    // existing ["S","M","L"] vs newer ["M","L","XL"]: "S" is only in existing, "XL" is only in
    // newer → superset ["S","M","L","XL"] must be built from scratch.
    String existing = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"size\",\"type\":"
        + "{\"type\":\"enum\",\"name\":\"Size\",\"symbols\":[\"S\",\"M\",\"L\"]," + "\"extra-prop\":\"old\"}}]}";
    String newer = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"size\",\"type\":"
        + "{\"type\":\"enum\",\"name\":\"Size\",\"symbols\":[\"M\",\"L\",\"XL\"],"
        + "\"default\":\"L\",\"extra-prop\":\"new\"}}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(existing);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(newer);
    Schema superset = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Schema supersetEnum = superset.getField("size").schema();

    Assert.assertEquals(supersetEnum.getEnumSymbols(), Arrays.asList("S", "M", "L", "XL"));
    // newSchema's custom prop and default are carried onto the newly constructed schema.
    Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(supersetEnum, "extra-prop"), "\"new\"");
    Assert.assertEquals(supersetEnum.getEnumDefault(), "L");
  }

  @Test
  public void testSchemaMergeEnumSymbolsDivergedPreservesProps() {
    // When existingSchema has symbols not in newSchema the superset merges props from both:
    // - prop only in existingSchema → preserved in superset
    // - prop in both schemas → newSchema value wins
    // - prop only in newSchema → present in superset
    String existing = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"status\",\"type\":"
        + "{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"A\",\"B\",\"C\"],"
        + "\"old-only\":\"from-old\",\"shared\":\"old-val\"}}]}";
    String newer = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"status\",\"type\":"
        + "{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"A\",\"B\",\"D\"],"
        + "\"new-only\":\"from-new\",\"shared\":\"new-val\"}}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(existing);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(newer);
    Schema superset = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Schema supersetEnum = superset.getField("status").schema();

    Assert.assertEquals(supersetEnum.getEnumSymbols(), Arrays.asList("A", "B", "C", "D"));
    // prop only in existingSchema is preserved
    Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(supersetEnum, "old-only"), "\"from-old\"");
    // prop only in newSchema is present
    Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(supersetEnum, "new-only"), "\"from-new\"");
    // prop in both: newSchema value wins
    Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(supersetEnum, "shared"), "\"new-val\"");
  }

  @Test
  public void testSchemaMergeFields() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id1\",\"type\":\"double\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id2\",\"type\":\"int\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Assert.assertNotNull(s3.getField("id1"));
    Assert.assertNotNull(s3.getField("id2"));
  }

  @Test
  public void testSchemaMergeFieldsBadDefaults() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id1\",\"type\":\"float\", \"default\" : 0}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id2\",\"type\":\"int\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Assert.assertNotNull(s3.getField("id1"));
    Assert.assertNotNull(s3.getField("id2"));
  }

  @Test
  public void testSchemaMergeFixedSameSize() {
    // FIXED schemas with the same size but different custom properties: superset succeeds and
    // newSchema's properties take priority.
    String existing = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"hash\",\"type\":"
        + "{\"type\":\"fixed\",\"name\":\"MD5\",\"size\":16,\"algo\":\"md4\"}}]}";
    String newer = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"hash\",\"type\":"
        + "{\"type\":\"fixed\",\"name\":\"MD5\",\"size\":16,\"algo\":\"md5\"}}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(existing);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(newer);
    Schema superset = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);

    Schema fixedSchema = superset.getField("hash").schema();
    Assert.assertEquals(fixedSchema.getFixedSize(), 16);
    Assert.assertEquals(AvroCompatibilityHelper.getSchemaPropAsJsonString(fixedSchema, "algo"), "\"md5\"");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testSchemaMergeFixedDifferentSizeThrows() {
    // FIXED schemas with mismatched sizes are structurally incompatible — must throw.
    String existing = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"hash\",\"type\":"
        + "{\"type\":\"fixed\",\"name\":\"MD5\",\"size\":16}}]}";
    String newer = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"hash\",\"type\":"
        + "{\"type\":\"fixed\",\"name\":\"MD5\",\"size\":32}}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(existing);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(newer);
    AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testWithIncompatibleSchema() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"int\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);

    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));
    AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
  }

  @Test
  public void testSchemaMergeUnion() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"string\", \"int\", \"null\"], \"default\" : \"dflt\"},{\"name\":\"organization\",\"type\":\"string\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Assert.assertNotNull(s3.getField("company"));
    Assert.assertNotNull(s3.getField("organization"));
  }

  @Test
  public void testSchemaMergeUnionWithComplexItemType() {
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(loadFileAsString("UnionV1.avsc"));
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(loadFileAsString("UnionV2.avsc"));
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));
    Schema s3 = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Assert.assertNotNull(s3.getField("age"));
    Assert.assertNotNull(s3.getField("field"));
    Schema.Field subFieldInS2 = s2.getField("field");
    Schema.Field subFieldInS3 = s3.getField("field");
    Schema unionSubFieldInS2 = subFieldInS2.schema().getTypes().get(1);
    Schema unionSubFieldInS3 = subFieldInS3.schema().getTypes().get(1);
    Assert.assertEquals(unionSubFieldInS3, unionSubFieldInS2);
  }

  @Test
  public void testWithNewFieldArrayRecord() {
    String recordSchemaStr1 = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
        + "  \"namespace\" : \"com.linkedin.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"hits\",\n"
        + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : {\n"
        + "        \"type\" : \"record\",\n" + "        \"name\" : \"JobAlertHit\",\n" + "        \"fields\" : [ {\n"
        + "          \"name\" : \"memberId\",\n" + "          \"type\" : \"long\"\n" + "        }, {\n"
        + "          \"name\" : \"searchId\",\n" + "          \"type\" : \"long\"\n" + "        } ]\n" + "      }\n"
        + "    },\n" + "    \"default\" : [ ]\n" + "  }, {\n" + "    \"name\" : \"hasNext\",\n"
        + "    \"type\" : \"boolean\",\n" + "    \"default\" : false\n" + "  } ]\n" + "}";

    String recordSchemaStr2 = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
        + "  \"namespace\" : \"com.linkedin.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"hits\",\n"
        + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : {\n"
        + "        \"type\" : \"record\",\n" + "        \"name\" : \"JobAlertHit\",\n" + "        \"fields\" : [ {\n"
        + "          \"name\" : \"memberId\",\n" + "          \"type\" : \"long\"\n" + "        }, {\n"
        + "          \"name\" : \"companyId\",\n" + "          \"type\" : \"long\"\n" + "        }, {\n"
        + "          \"name\" : \"searchId\",\n" + "          \"type\" : \"long\"\n" + "        } ]\n" + "      }\n"
        + "    },\n" + "    \"default\" : [ ]\n" + "  }, {\n" + "    \"name\" : \"hasNext\",\n"
        + "    \"type\" : \"boolean\",\n" + "    \"default\" : false\n" + "  } ]\n" + "}";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(recordSchemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(recordSchemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Assert.assertNotNull(s3);
  }

  @Test
  public void tesMergeWithDefaultValueUpdate() {
    String schemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"long\", \"default\" : 123}\n" + "           ]\n"
        + "        }";
    String schemaStr2 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                } \n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"long\", \"default\": 123}" + "           ]\n"
        + "        }";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Schema s3 = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Assert.assertNotNull(AvroSchemaUtils.getFieldDefault(s3.getField("salary")));
  }

  @Test
  public void testWithEnumEvolution() {
    // s1 has HEART, s2 does not. The superset must preserve HEART (from existing) and keep s2's
    // symbols in their original order — i.e. all four symbols are present.
    String schemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                } \n" + "              }\n" + "           ]\n" + "        }";
    String schemaStr2 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\"]\n"
        + "                } \n" + "              }\n" + "           ]\n" + "        }";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);

    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));
    Schema superset = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    // Superset symbols: existing order first (SPADES, DIAMONDS, HEART, CLUBS), nothing new from s2
    Assert.assertEquals(
        superset.getField("Suit").schema().getEnumSymbols(),
        Arrays.asList("SPADES", "DIAMONDS", "HEART", "CLUBS"));
  }

  @Test
  public void testIsSupersetSchemaSimpleRecordSchema() {
    String valueSchemaStr1 = "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   "
        + "  \"name\": \"User\",     " + "  \"fields\": [           "
        + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"}  " + "  ] " + " } ";

    String valueSchemaStr2 =
        "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
            + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  "
            + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"}  " + "  ] " + " } ";

    String valueSchemaStr3 = "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   "
        + "  \"name\": \"User\",     " + "  \"fields\": [           "
        + "       { \"name\": \"address\", \"type\": \"string\", \"default\": \"italy\"},  "
        + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"}  " + "  ] " + " } ";

    String valueSchemaStr4 = "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   "
        + "  \"name\": \"User\",     " + "  \"fields\": [           "
        + "       { \"name\": \"address\", \"type\": \"string\", \"default\": \"italy\"},  "
        + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  "
        + "       { \"name\": \"weight\", \"type\": \"float\", \"default\": 0.0}  " + "  ] " + " } ";

    Schema schema1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr1);
    Schema schema2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr2);
    Schema schema3 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr3);
    Schema schema4 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr4);

    // Case 1: a schema must be its own superset schema.
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema1));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema2));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema3, schema3));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema4, schema4));

    // Case 2: check is-superset-schema relation between different schemas.
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema1));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema2));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema3, schema2));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema3, schema1));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema3));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema3));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema3, schema4));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema4, schema3));
  }

  @Test
  public void testIsSupersetSchemaNestedRecordSchema() {
    // Both schemas have a field that is a Record. The inner Record field in the second schema has one more field than
    // that in the first schema. The second schema should be the first schema's superset schema.

    String valueSchemaStr1 = "{" + "    " + "\"name\": \"person\"," + "  \"type\": \"record\"," + "  \"fields\": ["
        + "     {\"name\": \"firstname\", \"type\": \"string\", \"default\": \"john\"},"
        + "     {\"name\": \"lastname\", \"type\": \"string\",  \"default\": \"doe\"}," + "     {"
        + "     \"name\": \"address\"," + "        \"type\": {" + "                   \"type\" : \"record\","
        + "                   \"name\" : \"AddressUSRecord\"," + "                   \"fields\" : ["
        + "                                  {\"name\": \"streetaddress\", \"type\": \"string\",  \"default\": \"101 XX\"},"
        + "                                  {\"name\": \"city\", \"type\": \"string\",  \"default\": \"Sunnyvale\"}"
        + "                                ]" + "                  }" + "     }" + "   ]" + "}";

    String valueSchemaStr2 = "{" + "    " + "\"name\": \"person\"," + "  \"type\": \"record\"," + "  \"fields\": ["
        + "     {\"name\": \"firstname\", \"type\": \"string\", \"default\": \"john\"},"
        + "     {\"name\": \"lastname\", \"type\": \"string\",  \"default\": \"doe\"}," + "     {"
        + "     \"name\": \"address\"," + "        \"type\": {" + "                   \"type\" : \"record\","
        + "                   \"name\" : \"AddressUSRecord\"," + "                   \"fields\" : ["
        + "                                  {\"name\": \"streetaddress\", \"type\": \"string\",  \"default\": \"101 XX\"},"
        + "                                  {\"name\": \"city\", \"type\": \"string\",  \"default\": \"Sunnyvale\"},"
        + "                                  {\"name\": \"country\", \"type\": \"string\",  \"default\": \"U.S.A\"}"
        + "                                ]" + "                  }" + "     }" + "   ]" + "}";

    Schema schema1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr1);
    Schema schema2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr2);

    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema1));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema2));
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(schema2, schema1));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(schema1, schema2));
  }

  @Test
  public void testSupersetSchemaContainsMergeFieldProps() {
    String valueSchemaStr1 = "{\n" + "  \"name\": \"TestRecord\",\n" + "  \"type\": \"record\",\n" + "  \"fields\": [\n"
        + "   {\"name\": \"int_field\", \"type\": \"int\", \"doc\": \"int field\", \"prop1\": \"\\\"prop1_v1\\\"\"}\n"
        + "  ],\n" + "  \"schema_prop\": \"\\\"schema_prop_v1\\\"\"\n" + "}";
    String valueSchemaStr2 = "{\n" + "  \"name\": \"TestRecord\",\n" + "  \"type\": \"record\",\n" + "  \"fields\": [\n"
        + "   {\"name\": \"int_field\", \"type\": \"int\", \"doc\": \"int field\", \"prop1\": \"\\\"prop1_v2\\\"\", \"prop2\": \"\\\"prop2_v1\\\"\"},\n"
        + "   {\"name\": \"string_field\", \"type\": \"string\", \"doc\": \"string field\", \"prop3\": \"\\\"prop3_v1\\\"\", \"prop2\": \"\\\"prop2_v2\\\"\"}\n"
        + "  ],\n" + "  \"schema_prop\": \"\\\"schema_prop_v2\\\"\"\n" + "}";

    Schema schema1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr1);
    Schema schema2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr2);
    Schema supersetSchema = AvroSupersetSchemaUtils.generateSupersetSchema(schema1, schema2);

    Schema.Field intField = supersetSchema.getField("int_field");
    Schema.Field stringField = supersetSchema.getField("string_field");

    Assert.assertEquals(intField.getProp("prop1"), "\"prop1_v2\"");
    Assert.assertEquals(intField.getProp("prop2"), "\"prop2_v1\"");
    Assert.assertEquals(stringField.getProp("prop3"), "\"prop3_v1\"");
    Assert.assertEquals(stringField.getProp("prop2"), "\"prop2_v2\"");
  }

  @Test
  public void testGetSupersetSchemaFromSchemaResponse() {
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[3];
    schemas[0] = new MultiSchemaResponse.Schema();
    schemas[0].setId(1);
    schemas[0].setSchemaStr("dummySchemaStr");
    schemas[1] = new MultiSchemaResponse.Schema();
    schemas[1].setId(1);
    schemas[1].setDerivedSchemaId(1);
    schemas[1].setSchemaStr("dummySchemaStr");
    schemas[2] = new MultiSchemaResponse.Schema();
    schemas[2].setId(2);
    schemas[2].setSchemaStr("dummySchemaStr2");
    MultiSchemaResponse schemaResponse = new MultiSchemaResponse();
    schemaResponse.setSchemas(schemas);

    MultiSchemaResponse.Schema retrievedSchema =
        AvroSupersetSchemaUtils.getSupersetSchemaFromSchemaResponse(schemaResponse, 2);

    Assert.assertNotNull(retrievedSchema);
    Assert.assertEquals(retrievedSchema.getSchemaStr(), "dummySchemaStr2");
  }

  @Test
  public void testGetLatestUpdateSchemaFromSchemaResponse() {
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[3];
    schemas[0] = new MultiSchemaResponse.Schema();
    schemas[0].setId(1);
    schemas[0].setSchemaStr("dummySchemaStr");
    schemas[1] = new MultiSchemaResponse.Schema();
    schemas[1].setId(1);
    schemas[1].setDerivedSchemaId(1);
    schemas[1].setSchemaStr("dummySchemaStr1");
    schemas[2] = new MultiSchemaResponse.Schema();
    schemas[2].setId(1);
    schemas[2].setDerivedSchemaId(2);
    schemas[2].setSchemaStr("dummySchemaStr2");
    MultiSchemaResponse schemaResponse = new MultiSchemaResponse();
    schemaResponse.setSchemas(schemas);

    MultiSchemaResponse.Schema retrievedSchema =
        AvroSupersetSchemaUtils.getLatestUpdateSchemaFromSchemaResponse(schemaResponse, 1);
    Assert.assertNotNull(retrievedSchema);
    Assert.assertEquals(retrievedSchema.getSchemaStr(), "dummySchemaStr2");
  }

  @Test
  public void testSupersetSchemaKeepDefault() {
    Assert.assertEquals(
        AvroSupersetSchemaUtils.generateSupersetSchema(USER_WITH_DEFAULT_SCHEMA, USER_SCHEMA).toString(),
        USER_WITH_DEFAULT_SCHEMA.toString());
    Assert.assertEquals(
        AvroSupersetSchemaUtils.generateSupersetSchema(USER_SCHEMA, USER_WITH_DEFAULT_SCHEMA).toString(),
        USER_WITH_DEFAULT_SCHEMA.toString());

    // Test nested record default value carry in both direction.
    Assert.assertEquals(
        AvroSupersetSchemaUtils
            .generateSupersetSchema(USER_WITH_NESTED_RECORD_AND_DEFAULT_SCHEMA, USER_WITH_NESTED_RECORD_SCHEMA)
            .toString(),
        USER_WITH_NESTED_RECORD_AND_DEFAULT_SCHEMA.toString());
    Assert.assertEquals(
        AvroSupersetSchemaUtils
            .generateSupersetSchema(USER_WITH_NESTED_RECORD_SCHEMA, USER_WITH_NESTED_RECORD_AND_DEFAULT_SCHEMA)
            .toString(),
        USER_WITH_NESTED_RECORD_AND_DEFAULT_SCHEMA.toString());
  }

  @Test
  public void testGenerateSupersetSchemaWithAnnotatedPrimitiveTypesAndNewEnumField() {
    // v1: plain primitive types throughout.
    // - InnerRecord.tag uses a plain null in its union (covers NULL type)
    // - DateRecord uses plain int/long/boolean/float/double/bytes (covers remaining primitive types)
    String schemaV1 = "{" + "\"type\":\"record\",\"name\":\"OuterRecord\",\"namespace\":\"com.example\","
        + "\"fields\":[" + "  {\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{"
        + "    \"type\":\"record\",\"name\":\"InnerRecord\"," + "    \"fields\":["
        + "      {\"name\":\"id\",\"type\":\"string\"},"
        // NULL: plain null in union
        + "      {\"name\":\"tag\",\"type\":[\"null\",\"string\"],\"default\":null}," + "      {\"name\":\"subItems\","
        + "       \"type\":[\"null\",{\"type\":\"array\",\"items\":{"
        + "         \"type\":\"record\",\"name\":\"SubItem\"," + "         \"fields\":["
        + "           {\"name\":\"ref\",\"type\":\"string\"},"
        + "           {\"name\":\"categoryRef\",\"type\":[\"null\",\"string\"],\"default\":null}" + "         ]"
        + "       }}],\"default\":null}," + "      {\"name\":\"startDate\"," + "       \"type\":[\"null\",{"
        + "         \"type\":\"record\",\"name\":\"DateRecord\",\"namespace\":\"com.example.common\","
        + "         \"fields\":["
        // INT
        + "           {\"name\":\"year\",\"type\":[\"null\",\"int\"]},"
        + "           {\"name\":\"month\",\"type\":[\"null\",\"int\"]},"
        + "           {\"name\":\"day\",\"type\":[\"null\",\"int\"]},"
        // LONG
        + "           {\"name\":\"timestamp\",\"type\":[\"null\",\"long\"]},"
        // BOOLEAN
        + "           {\"name\":\"active\",\"type\":[\"null\",\"boolean\"]},"
        // FLOAT
        + "           {\"name\":\"score\",\"type\":[\"null\",\"float\"]},"
        // DOUBLE
        + "           {\"name\":\"weight\",\"type\":[\"null\",\"double\"]},"
        // BYTES
        + "           {\"name\":\"data\",\"type\":[\"null\",\"bytes\"]}" + "         ]" + "       }],\"default\":null},"
        + "      {\"name\":\"endDate\",\"type\":[\"null\",\"com.example.common.DateRecord\"],\"default\":null}"
        + "    ]" + "  }}}" + "]}";

    // v2: SubItem gains a new nullable enum field "source" (default null); all primitive types in
    // InnerRecord and DateRecord carry a custom "proto.fieldType" property — exercising superset
    // merge and comparison for every annotated primitive type (NULL, INT, LONG, BOOLEAN, FLOAT,
    // DOUBLE, BYTES).
    String schemaV2 = "{" + "\"type\":\"record\",\"name\":\"OuterRecord\",\"namespace\":\"com.example\","
        + "\"fields\":[" + "  {\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{"
        + "    \"type\":\"record\",\"name\":\"InnerRecord\"," + "    \"fields\":["
        + "      {\"name\":\"id\",\"type\":\"string\"},"
        // NULL: annotated null in union
        + "      {\"name\":\"tag\",\"type\":[{\"type\":\"null\",\"proto.nullable\":\"true\"},\"string\"],\"default\":null},"
        + "      {\"name\":\"subItems\"," + "       \"type\":[\"null\",{\"type\":\"array\",\"items\":{"
        + "         \"type\":\"record\",\"name\":\"SubItem\"," + "         \"fields\":["
        + "           {\"name\":\"ref\",\"type\":\"string\"},"
        + "           {\"name\":\"categoryRef\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "           {\"name\":\"source\"," + "            \"type\":[\"null\",{"
        + "              \"type\":\"enum\",\"name\":\"SourceType\","
        + "              \"symbols\":[\"UNKNOWN\",\"EXPLICIT\",\"DERIVED\",\"INFERRED\"],"
        + "              \"default\":\"UNKNOWN\"" + "            }],\"default\":null}" + "         ]"
        + "       }}],\"default\":null}," + "      {\"name\":\"startDate\"," + "       \"type\":[\"null\",{"
        + "         \"type\":\"record\",\"name\":\"DateRecord\",\"namespace\":\"com.example.common\","
        + "         \"fields\":["
        // INT
        + "           {\"name\":\"year\",\"type\":[\"null\",{\"type\":\"int\",\"proto.fieldType\":\"sint32\"}],\"proto.fieldNumber\":1},"
        + "           {\"name\":\"month\",\"type\":[\"null\",{\"type\":\"int\",\"proto.fieldType\":\"sint32\"}],\"proto.fieldNumber\":2},"
        + "           {\"name\":\"day\",\"type\":[\"null\",{\"type\":\"int\",\"proto.fieldType\":\"sint32\"}],\"proto.fieldNumber\":3},"
        // LONG
        + "           {\"name\":\"timestamp\",\"type\":[\"null\",{\"type\":\"long\",\"proto.fieldType\":\"int64\"}],\"proto.fieldNumber\":4},"
        // BOOLEAN
        + "           {\"name\":\"active\",\"type\":[\"null\",{\"type\":\"boolean\",\"proto.fieldType\":\"bool\"}],\"proto.fieldNumber\":5},"
        // FLOAT
        + "           {\"name\":\"score\",\"type\":[\"null\",{\"type\":\"float\",\"proto.fieldType\":\"float\"}],\"proto.fieldNumber\":6},"
        // DOUBLE
        + "           {\"name\":\"weight\",\"type\":[\"null\",{\"type\":\"double\",\"proto.fieldType\":\"double\"}],\"proto.fieldNumber\":7},"
        // BYTES
        + "           {\"name\":\"data\",\"type\":[\"null\",{\"type\":\"bytes\",\"proto.fieldType\":\"bytes\"}],\"proto.fieldNumber\":8}"
        + "         ]" + "       }],\"default\":null},"
        + "      {\"name\":\"endDate\",\"type\":[\"null\",\"com.example.common.DateRecord\"],\"default\":null}"
        + "    ]" + "  }}}" + "]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaV1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaV2);

    // v1 and v2 differ structurally (SubItem gains a "source" field in v2)
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    // Superset should be generated successfully in both directions
    Schema supersetS1S2 = AvroSupersetSchemaUtils.generateSupersetSchema(s1, s2);
    Assert.assertNotNull(supersetS1S2);

    Schema supersetS2S1 = AvroSupersetSchemaUtils.generateSupersetSchema(s2, s1);
    Assert.assertNotNull(supersetS2S1);

    // Both orderings should produce equivalent superset schemas
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(supersetS1S2, supersetS2S1));

    // v2 (which contains the "source" field) should be the superset of v1
    Assert.assertTrue(AvroSupersetSchemaUtils.isSupersetSchema(s2, s1));
    Assert.assertFalse(AvroSupersetSchemaUtils.isSupersetSchema(s1, s2));

    // The superset schema should contain "source" inside SubItem, and retain "categoryRef" from v1.
    // Navigation: items (array items -> InnerRecord) -> subItems (union[1] -> array items -> SubItem)
    Schema subItemSchema = supersetS1S2.getField("items")
        .schema()
        .getElementType()
        .getField("subItems")
        .schema()
        .getTypes()
        .get(1)
        .getElementType();
    Assert.assertNotNull(subItemSchema.getField("source"), "Superset schema must contain the 'source' field from v2");
    Assert.assertNotNull(
        subItemSchema.getField("categoryRef"),
        "Superset schema must retain the 'categoryRef' field from v1");
  }

  @Test
  public void testValidateSubsetSchema() {
    Assert.assertTrue(
        AvroSupersetSchemaUtils.validateSubsetValueSchema(NAME_RECORD_V1_SCHEMA, NAME_RECORD_V2_SCHEMA.toString()));
    Assert.assertFalse(
        AvroSupersetSchemaUtils.validateSubsetValueSchema(NAME_RECORD_V2_SCHEMA, NAME_RECORD_V3_SCHEMA.toString()));
    Assert.assertFalse(
        AvroSupersetSchemaUtils.validateSubsetValueSchema(NAME_RECORD_V3_SCHEMA, NAME_RECORD_V4_SCHEMA.toString()));

    // NAME_RECORD_V5_SCHEMA and NAME_RECORD_V6_SCHEMA are different in props for field.
    Assert.assertNotEquals(NAME_RECORD_V5_SCHEMA, NAME_RECORD_V6_SCHEMA);
    // Test validation skip comparing props when checking for subset schema.
    Schema supersetSchemaForV5AndV4 =
        AvroSupersetSchemaUtils.generateSupersetSchema(NAME_RECORD_V5_SCHEMA, NAME_RECORD_V4_SCHEMA);
    Assert.assertTrue(
        AvroSupersetSchemaUtils.validateSubsetValueSchema(NAME_RECORD_V5_SCHEMA, supersetSchemaForV5AndV4.toString()));
    Assert.assertTrue(
        AvroSupersetSchemaUtils.validateSubsetValueSchema(NAME_RECORD_V6_SCHEMA, supersetSchemaForV5AndV4.toString()));
  }
}
