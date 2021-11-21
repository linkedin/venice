package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.AvroSchemaUtils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroSchemaVerifier;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAvroSchemaUtils {
  @Test
  public void testWithDifferentDocField() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";
    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3);
  }

  @Test
  public void testStringVsAvroString() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\": {\"type\" : \"string\", \"avro.java.string\" : \"String\"},\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";

    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);

    Assert.assertFalse(s2.equals(s1));
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSchemaUtils.generateSuperSetSchema(s2, s1);
    Assert.assertNotNull(s3);
    Assert.assertNotNull(AvroCompatibilityHelper.getSchemaPropAsJsonString(s3.getField("name").schema(), "avro.java.string"));
  }

  @Test
  public void testCompareWithDifferentOrderFields() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"company\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"name\",\"type\":\"string\"}]}";

    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);

    AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));
  }

  @Test
  public void testCompareWithDifferentOrderFieldsNested() {
    String recordSchemaStr1 = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ]\n"
        + "      }\n" +
        "    },\n" +
        "    \"default\" : [ ]\n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";
    String recordSchemaStr2 = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ]\n"
        + "      }\n" +
        "    },\n" +
        "    \"default\" : [ ]\n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";


    Schema s1 = Schema.parse(recordSchemaStr1);
    Schema s2 = Schema.parse(recordSchemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testWithIncompatibleSchema() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"int\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";
    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);

    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));
    AvroSchemaUtils.generateSuperSetSchema(s1, s2);
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testWithEnumEvolution() {
    String schemaStr1 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n" +
        "                } \n" +
        "              }\n" +
        "           ]\n" +
        "        }";
    String schemaStr2 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\"]\n" +
        "                } \n" +
        "              }\n" +
        "           ]\n" +
        "        }";
    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);

    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));
    AvroSchemaUtils.generateSuperSetSchema(s1, s2);
  }

  @Test
  public void testSchemaMerge() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"business\",\"type\":\"string\"}]}";

    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));
    Schema s3 = AvroSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3);
  }

  @Test
  public void testSchemaMergeUnion() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"string\", \"int\", \"null\"], \"default\" : \"dflt\"},{\"name\":\"organization\",\"type\":\"string\"}]}";

    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3.getField("company"));
    Assert.assertNotNull(s3.getField("organization"));
  }

  @Test
  public void testSchemaUnionDefaultValidation() {
    String schemaStr = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32},{\"name\":\"company\",\"type\":\"string\"}]}";

    AvroSchemaUtils.validateAvroSchemaStr(schemaStr);
    Assert.assertTrue(AvroSchemaUtils.isValidAvroSchema(Schema.parse(schemaStr)));

    schemaStr = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : null},{\"name\":\"company\",\"type\":\"string\"}]}";

    try {
      Assert.assertFalse(AvroSchemaUtils.isValidAvroSchema(Schema.parse(schemaStr)));
      AvroSchemaUtils.validateAvroSchemaStr(schemaStr);
      Assert.fail("Default null should fail with int first union field");
    } catch (AvroTypeException e) {
      Assert.assertEquals(e.getMessage(), "Invalid default for field experience: null not a [\"int\",\"float\",\"null\"]");
    } catch (InvalidVeniceSchemaException e) {
      Assert.assertEquals(e.getMessage(), "Union field KeyRecord.experience has invalid default value." + ""
          + " A union's default value type should match the first branch of the union." + ""
          + " Excepting int as its the first branch of : [\"int\",\"float\",\"null\"] instead got null");
    }

    schemaStr = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\", \"default\": \"default_name\"},{\"name\":\"experience\",\"type\":[\"null\", \"int\", \"float\"], \"default\" : null},{\"name\":\"company\",\"type\":\"string\"}]}";
    AvroSchemaUtils.validateAvroSchemaStr(schemaStr);

    schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : [ {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ]\n"
        + "      }]\n" +
        "    },\n" +
        "    \"default\" :  [ ] \n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";

    AvroSchemaVerifier.get().verifyCompatibility(Schema.parse(schemaStr), Schema.parse(schemaStr));

    AvroSchemaUtils.validateAvroSchemaStr(schemaStr);

  }

  @Test
  public void testSchemaMergeFields() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id1\",\"type\":\"double\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id2\",\"type\":\"int\"}]}";

    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3.getField("id1"));
    Assert.assertNotNull(s3.getField("id2"));
  }

  @Test
  public void testWithNewFieldArrayRecord() {
    String recordSchemaStr1 = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ]\n"
        + "      }\n" +
        "    },\n" +
        "    \"default\" : [ ]\n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";

    String recordSchemaStr2 = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"companyId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ]\n"
        + "      }\n" +
        "    },\n" +
        "    \"default\" : [ ]\n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";
    Schema s1 = Schema.parse(recordSchemaStr1);
    Schema s2 = Schema.parse(recordSchemaStr2);
    Assert.assertFalse(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(s3);
  }

  @Test
  public void tesMergeWithDefaultValueUpdate() {
    String schemaStr1 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n" +
        "                }\n" +
        "              },\n" +
        "               {\"name\": \"salary\", \"type\": \"long\", \"default\" : 123}\n" +
        "           ]\n" +
        "        }";
    String schemaStr2 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n" +
        "                } \n" +
        "              },\n" +
        "               {\"name\": \"salary\", \"type\": \"long\", \"default\": 123}" +
        "           ]\n" +
        "        }";

    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Schema s3 = AvroSchemaUtils.generateSuperSetSchema(s1, s2);
    Assert.assertNotNull(AvroSchemaUtils.getFieldDefault(s3.getField("salary")));
  }

  @Test
  public void testDocChange() {
    String schemaStr1 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n" +
        "                }\n" +
        "              },\n" +
        "               {\"name\": \"salary\", \"type\": \"long\"}\n" +
        "           ]\n" +
        "        }";
    String schemaStr2 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n" +
        "                }\n" +
        "              },\n" +
        "               {\"name\": \"salary\", \"type\": \"long\"}\n" +
        "           ]\n" +
        "        }";
    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Assert.assertTrue(s1.equals(s2));
    Assert.assertFalse(AvroSchemaUtils.hasDocFieldChange(s1, s2));
  }

  @Test
  public void testSchemaUnionDocUpdate() {
    String schemaStr1 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32, \"doc\" : \"doc\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field1\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : 32, \"doc\" : \"doc\"},{\"name\":\"company\",\"type\":\"string\"}]}";

    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);
    Assert.assertTrue(s1.equals(s2));
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Assert.assertTrue(AvroSchemaUtils.hasDocFieldChange(s1, s2));
  }

  @Test
  public void testSchemaArrayDocUpdate() {
    String schemaStr1 = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ], \n" +
        "        \"doc\" : \"record doc\" \n" +
        "      }\n" +
        "    },\n" +
        "    \"default\" : [ ]\n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";

    String schemaStr2 = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"namespace\" : \"com.linkedin.avro\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"hits\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : {\n" +
        "        \"type\" : \"record\",\n" +
        "        \"name\" : \"JobAlertHit\",\n" +
        "        \"fields\" : [ {\n" +
        "          \"name\" : \"memberId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        }, {\n" +
        "          \"name\" : \"searchId\",\n" +
        "          \"type\" : \"long\"\n" +
        "        } ]\n"
        + "      }\n" +
        "    },\n" +
        "    \"default\" : [ ]\n" +
        "  }, {\n" +
        "    \"name\" : \"hasNext\",\n" +
        "    \"type\" : \"boolean\",\n" +
        "    \"default\" : false\n" +
        "  } ]\n" +
        "}";

    Schema s1 = Schema.parse(schemaStr1);
    Schema s2 = Schema.parse(schemaStr2);
    Assert.assertTrue(s1.equals(s2));
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1,s2));

    Assert.assertTrue(AvroSchemaUtils.hasDocFieldChange(s1, s2));
  }

  @Test
  public void testSchemaArrayDocUpdateNestedFieldArray() {
    String str1 =
        "{\"type\":\"record\",\"name\":\"memberFeaturesObject\",\"namespace\":\"com.linkedin.venice.schemas\",\"fields\":[{\"name\":\"visitProbabilityForInvite\",\"type\":\"float\",\"doc\":\"The probability of a member visiting the site after sending this member an invitation. The range of this value will between 0 to 1. The higher the value, the more likely that member will visit the site. \",\"default\":-1},{\"name\":\"activityLevel\",\"type\":\"int\",\"doc\":\"Member activity level, from 0-4. 0-4 means: 0(Onboarding), 1(Dormant), 2(OneByOne), 3(OneByThree), 4(FourbyFour)\",\"default\":0},{\"name\":\"connectionCount\",\"type\":\"int\",\"doc\":\"The total connection count of a member.\",\"default\":0},{\"name\":\"lastConnectionAcceptedAt\",\"type\":\"int\",\"doc\":\"The Unix timestamp (seconds) of the most recently accepted connection.\",\"default\":0},{\"name\":\"pVisitModel\",\"type\":\"string\",\"doc\":\"Different lix treatment(pvisit models) for computing pvisit score.\",\"default\":\"\"},{\"name\":\"pVisitBasicModelScore\",\"type\":\"float\",\"doc\":\"The score is computed based on the pVisit basic model which is simply member visit probability.\",\"default\":0},{\"name\":\"pVisitEqmGainModelScore\",\"type\":\"float\",\"doc\":\"The pvisit score is computed based on a model that uses potential eQM gain as weight during model training. eQM_gain model slightly prefer member visit from who has more eQM gain.\",\"default\":0},{\"name\":\"contribLevel\",\"type\":\"int\",\"doc\":\"Whether the user contribute in the last k days (k=7).\",\"default\":0},{\"name\":\"invReceivedCount\",\"type\":\"int\",\"doc\":\"The number of member connection invitations received by the member in the past 7 days.\",\"default\":0},{\"name\":\"publicContributor\",\"type\":\"int\",\"doc\":\"The member has like/commented/shared or created content that has viral action\",\"default\":0},{\"name\":\"privateContributor\",\"type\":\"int\",\"doc\":\"The member has contributed through private messaging or inMail\",\"default\":0},{\"name\":\"totalActiveConnections\",\"type\":\"int\",\"doc\":\"The number of connections of a member who have made at least 1 contribution (like/comment/share/message) in the past 7 days\",\"default\":0},{\"name\":\"privateActiveConnections\",\"type\":\"int\",\"doc\":\"The number of connections of a member who have made at least 1 private contribution (message/inMail) in the past 7 days\",\"default\":0},{\"name\":\"publicActiveConnections\",\"type\":\"int\",\"doc\":\"The number of connections of a member who have made at least 1 public contribution (like/comment/share/publish) in the past 7 days\",\"default\":0},{\"name\":\"allContributionCount\",\"type\":\"int\",\"doc\":\"The total number of contributions (likes, comments, shares, private messages/inmails) that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"likeCount\",\"type\":\"int\",\"doc\":\"The number of likes that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"commentCount\",\"type\":\"int\",\"doc\":\"The number of comments that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"shareCount\",\"type\":\"int\",\"doc\":\"The number of shares that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"articleContribCount\",\"type\":\"int\",\"doc\":\"The total number of articles that a member has contributed\",\"default\":0},{\"name\":\"privateContribCount\",\"type\":\"int\",\"doc\":\"The number of private messages or inMails that the member has sent in the past 7 days\",\"default\":0},{\"name\":\"allFeedContribCount\",\"type\":\"int\",\"doc\":\"The total number of contributions(likes, comments, shares) in Feed that the member contributed in the past 7 days\",\"default\":0},{\"name\":\"feedLikeCount\",\"type\":\"int\",\"doc\":\"The total number of likes that the member contributed in Feed in the past 7 days\",\"default\":0},{\"name\":\"feedCommentCount\",\"type\":\"int\",\"doc\":\"The total number of comments that the member contributed in Feed in the past 7 days\",\"default\":0},{\"name\":\"feedShareCount\",\"type\":\"int\",\"doc\":\"The total number of shares that the member contributed in Feed in the past 7 days\",\"default\":0},{\"name\":\"evDestLix\",\"type\":\"int\",\"doc\":\"The member is a targeted dest candidate of edge value model\",\"default\":0},{\"name\":\"mfInviter\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The inviter member embedding computed by matrix factorization\",\"default\":[]},{\"name\":\"mfInvitee\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The invitee member embedding computed by matrix factorization\",\"default\":[]},{\"name\":\"boostFactorConsumerOptimus\",\"type\":\"float\",\"doc\":\"Boost factor to be applied to all destinations -- optimus consumer-side experiment\",\"default\":0},{\"name\":\"utilityConsumerOptimus\",\"type\":\"float\",\"doc\":\"Utility score for being destination -- optimus consumer-side experiment\",\"default\":0},{\"name\":\"skillsEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member skills embedding\",\"default\":[]},{\"name\":\"titlesEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member titles embedding\",\"default\":[]},{\"name\":\"companiesEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member companies embedding\",\"default\":[]},{\"name\":\"schoolsEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member schools embedding\",\"default\":[]},{\"name\":\"needleEmbedding\",\"type\":{\"type\":\"array\",\"items\":\"float\"},\"doc\":\"The member needle embedding\",\"default\":[]},{\"name\":\"profileCompanies\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"profileCompanies\",\"fields\":[{\"name\":\"companyId\",\"type\":\"int\",\"doc\":\"\",\"default\":0},{\"name\":\"industryId\",\"type\":\"int\",\"doc\":\"\",\"default\":0}]}},\"doc\":\"Profile company info\",\"default\":[]},{\"name\":\"profileSchools\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"profileSchools\",\"fields\":[{\"name\":\"schoolId\",\"type\":\"int\",\"doc\":\"\",\"default\":0},{\"name\":\"cipCodeId\",\"type\":\"string\",\"doc\":\"\",\"default\":\"\"},{\"name\":\"degreeId\",\"type\":\"int\",\"doc\":\"\",\"default\":0},{\"name\":\"startTime\",\"type\":\"long\",\"doc\":\"\",\"default\":-1}]}},\"doc\":\"Profile school info\",\"default\":[]},{\"name\":\"profileGeo\",\"type\":\"string\",\"doc\":\"Profile geo\",\"default\":\"\"},{\"name\":\"memberLanguages\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"Inferred languages over a threshold for a member.\",\"default\":[]},{\"name\":\"trianglePcreate\",\"type\":\"float\",\"doc\":\"Member feedback sensitivity (for creation)\",\"default\":0},{\"name\":\"deltaPcreate\",\"type\":\"float\",\"doc\":\"Member feedback delta sensitivity (for creation)\",\"default\":0}]}";
    Schema s1 = Schema.parse(str1);
    Schema s2 = Schema.parse(str1);
    Assert.assertTrue(s1.equals(s2));
    Schema schema = AvroSchemaUtils.generateSuperSetSchema(s1,s2);
    Assert.assertEquals(schema, s1);
    Assert.assertTrue(AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2));

    Assert.assertFalse(AvroSchemaUtils.hasDocFieldChange(s1, s2));
  }
}
