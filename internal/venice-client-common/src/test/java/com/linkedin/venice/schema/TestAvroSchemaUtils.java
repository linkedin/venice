package com.linkedin.venice.schema;

import com.linkedin.avroutil1.compatibility.AvroSchemaVerifier;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAvroSchemaUtils {
  @Test
  public void testCompareWithDifferentOrderFields() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"company\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"name\",\"type\":\"string\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);

    AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));
  }

  @Test
  public void testCompareWithDifferentOrderFieldsNested() {
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
        + "          \"name\" : \"searchId\",\n" + "          \"type\" : \"long\"\n" + "        }, {\n"
        + "          \"name\" : \"memberId\",\n" + "          \"type\" : \"long\"\n" + "        } ]\n" + "      }\n"
        + "    },\n" + "    \"default\" : [ ]\n" + "  }, {\n" + "    \"name\" : \"hasNext\",\n"
        + "    \"type\" : \"boolean\",\n" + "    \"default\" : false\n" + "  } ]\n" + "}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(recordSchemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(recordSchemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));
  }

  @Test
  public void testSchemaUnionDefaultValidation() {
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32},{\"name\":\"company\",\"type\":\"string\"}]}";

    AvroSchemaUtils.validateAvroSchemaStr(schemaStr);
    Assert.assertTrue(
        AvroSchemaUtils.isValidAvroSchema(AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr)));

    schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : null},{\"name\":\"company\",\"type\":\"string\"}]}";

    try {
      Assert.assertFalse(
          AvroSchemaUtils.isValidAvroSchema(AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr)));
      AvroSchemaUtils.validateAvroSchemaStr(schemaStr);
      Assert.fail("Default null should fail with int first union field");
    } catch (AvroTypeException e) {
      Assert.assertEquals(
          e.getMessage(),
          "Invalid default for field experience: null not a [\"int\",\"float\",\"null\"]");
    } catch (InvalidVeniceSchemaException e) {
      Assert.assertEquals(
          e.getMessage(),
          "Union field KeyRecord.experience has invalid default value." + ""
              + " A union's default value type should match the first branch of the union." + ""
              + " Excepting int as its the first branch of : [\"int\",\"float\",\"null\"] instead got null");
    }

    schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\", \"default\": \"default_name\"},{\"name\":\"experience\",\"type\":[\"null\", \"int\", \"float\"], \"default\" : null},{\"name\":\"company\",\"type\":\"string\"}]}";
    AvroSchemaUtils.validateAvroSchemaStr(schemaStr);

    schemaStr = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
        + "  \"namespace\" : \"com.linkedin.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"hits\",\n"
        + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : [ {\n"
        + "        \"type\" : \"record\",\n" + "        \"name\" : \"JobAlertHit\",\n" + "        \"fields\" : [ {\n"
        + "          \"name\" : \"memberId\",\n" + "          \"type\" : \"long\"\n" + "        }, {\n"
        + "          \"name\" : \"searchId\",\n" + "          \"type\" : \"long\"\n" + "        } ]\n" + "      }]\n"
        + "    },\n" + "    \"default\" :  [ ] \n" + "  }, {\n" + "    \"name\" : \"hasNext\",\n"
        + "    \"type\" : \"boolean\",\n" + "    \"default\" : false\n" + "  } ]\n" + "}";

    AvroSchemaVerifier.get()
        .verifyCompatibility(
            AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr),
            AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr));

    AvroSchemaUtils.validateAvroSchemaStr(schemaStr);

  }

  @Test
  public void testDocChange() {
    String schemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n" + "               {\"name\": \"salary\", \"type\": \"long\"}\n"
        + "           ]\n" + "        }";
    String schemaStr2 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n" + "               {\"name\": \"salary\", \"type\": \"long\"}\n"
        + "           ]\n" + "        }";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Assert.assertEquals(s2, s1);
    Assert.assertFalse(AvroSchemaUtils.hasDocFieldChange(s1, s2));
  }

  @Test
  public void testSchemaUnionDocUpdate() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32, \"doc\" : \"doc\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field1\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32, \"doc\" : \"doc\"},{\"name\":\"company\",\"type\":\"string\"}]}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertEquals(s2, s1);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Assert.assertTrue(AvroSchemaUtils.hasDocFieldChange(s1, s2));
  }

  @Test
  public void testSchemaArrayDocUpdate() {
    String schemaStr1 = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
        + "  \"namespace\" : \"com.linkedin.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"hits\",\n"
        + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : {\n"
        + "        \"type\" : \"record\",\n" + "        \"name\" : \"JobAlertHit\",\n" + "        \"fields\" : [ {\n"
        + "          \"name\" : \"memberId\",\n" + "          \"type\" : \"long\"\n" + "        }, {\n"
        + "          \"name\" : \"searchId\",\n" + "          \"type\" : \"long\"\n" + "        } ], \n"
        + "        \"doc\" : \"record doc\" \n" + "      }\n" + "    },\n" + "    \"default\" : [ ]\n" + "  }, {\n"
        + "    \"name\" : \"hasNext\",\n" + "    \"type\" : \"boolean\",\n" + "    \"default\" : false\n" + "  } ]\n"
        + "}";

    String schemaStr2 = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
        + "  \"namespace\" : \"com.linkedin.avro\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"hits\",\n"
        + "    \"type\" : {\n" + "      \"type\" : \"array\",\n" + "      \"items\" : {\n"
        + "        \"type\" : \"record\",\n" + "        \"name\" : \"JobAlertHit\",\n" + "        \"fields\" : [ {\n"
        + "          \"name\" : \"memberId\",\n" + "          \"type\" : \"long\"\n" + "        }, {\n"
        + "          \"name\" : \"searchId\",\n" + "          \"type\" : \"long\"\n" + "        } ]\n" + "      }\n"
        + "    },\n" + "    \"default\" : [ ]\n" + "  }, {\n" + "    \"name\" : \"hasNext\",\n"
        + "    \"type\" : \"boolean\",\n" + "    \"default\" : false\n" + "  } ]\n" + "}";

    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr2);
    Assert.assertEquals(s2, s1);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Assert.assertTrue(AvroSchemaUtils.hasDocFieldChange(s1, s2));
  }

  @Test(expectedExceptions = AvroTypeException.class, expectedExceptionsMessageRegExp = "Invalid default for field.*")
  public void testAvroValidDefaults() {
    String str =
        "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
            + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  "
            + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  "
            + "       { \"name\": \"weight\", \"type\": \"float\", \"default\": 0},  "
            + "       { \"name\": \"age\", \"type\": \"float\", \"default\": 0 }" + "  ] " + " } ";
    AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(str);
  }

  @Test
  public void testAvroCompareDefaults() {
    String str1 =
        "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
            + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  "
            + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  "
            + "       { \"name\": \"weight\", \"type\": \"float\", \"default\": 0},  "
            + "       { \"name\": \"age\", \"type\": \"float\", \"default\": 0.0 }" + "  ] " + " } ";
    String str2 =
        "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
            + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  "
            + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"venice\"},  "
            + "       { \"name\": \"weight\", \"type\": \"float\", \"default\": 0},  "
            + "       { \"name\": \"age\", \"type\": \"float\", \"default\": 0.0 }" + "  ] " + " } ";
    Schema s1 = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(str1);
    Schema s2 = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(str2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));
  }

  @Test
  public void testGetSchemaIdCanonicalMatch() {
    // Empty collection - should return INVALID_VALUE_SCHEMA_ID
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
    Schema schema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr);
    SchemaEntry targetEntry = new SchemaEntry(1, schema);

    int result = AvroSchemaUtils.getSchemaIdCanonicalMatch(Collections.emptyList(), targetEntry);
    Assert.assertEquals(result, SchemaData.INVALID_VALUE_SCHEMA_ID);

    // Single exact match - should return the matching schema ID
    List<SchemaEntry> schemas = new ArrayList<>();
    SchemaEntry entry1 = new SchemaEntry(10, schema);
    schemas.add(entry1);

    result = AvroSchemaUtils.getSchemaIdCanonicalMatch(schemas, targetEntry);
    Assert.assertEquals(result, 10);

    // Multiple canonicalized matches but only one exact match
    String schemaWithDoc =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"doc\":\"This is a doc\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
    Schema schemaWithDocParsed = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaWithDoc);
    SchemaEntry entryWithDoc = new SchemaEntry(20, schemaWithDocParsed);
    schemas.add(entryWithDoc);

    // Both schemas are canonically the same but not exactly the same due to doc field
    result = AvroSchemaUtils.getSchemaIdCanonicalMatch(schemas, targetEntry);
    Assert.assertEquals(result, 10); // Should return the exact match

    result = AvroSchemaUtils.getSchemaIdCanonicalMatch(schemas, entryWithDoc);
    Assert.assertEquals(result, 20); // Should return the exact match

    // Multiple canonicalized matches with multiple exact matches - should return largest ID
    SchemaEntry entry2 = new SchemaEntry(5, schema); // Same schema, smaller ID
    SchemaEntry entry3 = new SchemaEntry(15, schema); // Same schema, larger ID
    schemas.add(entry2);
    schemas.add(entry3);

    result = AvroSchemaUtils.getSchemaIdCanonicalMatch(schemas, targetEntry);
    Assert.assertEquals(result, 15); // Should return the largest ID among exact matches

    // Multiple canonicalized matches but no exact matches - should return largest ID from canonicalized matches
    schemas.clear();
    String schemaWithDoc1 =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"doc\":\"Doc 1\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
    String schemaWithDoc2 =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"doc\":\"Doc 2\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
    Schema schemaWithDoc1Parsed = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaWithDoc1);
    Schema schemaWithDoc2Parsed = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaWithDoc2);

    SchemaEntry entryWithDoc1 = new SchemaEntry(25, schemaWithDoc1Parsed);
    SchemaEntry entryWithDoc2 = new SchemaEntry(30, schemaWithDoc2Parsed);
    schemas.add(entryWithDoc1);
    schemas.add(entryWithDoc2);

    result = AvroSchemaUtils.getSchemaIdCanonicalMatch(schemas, targetEntry);
    Assert.assertEquals(result, 30); // Should return the largest ID among canonicalized matches

    // No matches at all - should return INVALID_VALUE_SCHEMA_ID
    schemas.clear();
    String differentSchemaStr =
        "{\"type\":\"record\",\"name\":\"DifferentRecord\",\"fields\":[{\"name\":\"field2\",\"type\":\"int\"}]}";
    Schema differentSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(differentSchemaStr);
    SchemaEntry differentEntry = new SchemaEntry(40, differentSchema);
    schemas.add(differentEntry);

    result = AvroSchemaUtils.getSchemaIdCanonicalMatch(schemas, targetEntry);
    Assert.assertEquals(result, SchemaData.INVALID_VALUE_SCHEMA_ID);
  }
}
