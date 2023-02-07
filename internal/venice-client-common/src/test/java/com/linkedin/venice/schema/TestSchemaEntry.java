package com.linkedin.venice.schema;

import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import org.apache.avro.SchemaParseException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSchemaEntry {
  @Test
  public void testToString() {
    String schemaStr = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"}\n" + "           ]\n" + "        }";
    SchemaEntry schemaEntry = new SchemaEntry(10, schemaStr);
    String expectedStr =
        "10\t{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}\tfalse";
    Assert.assertEquals(schemaEntry.toString(), expectedStr);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testInvalidSchemaWithInvalidJsonFormat() {
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\"::\"string\"}]}";
    new SchemaEntry(10, schemaStr);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testInvalidSchemaWithMissingField() {
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\"}]}";
    new SchemaEntry(10, schemaStr);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNullSchema() {
    new SchemaEntry(10, (String) null);
  }

  @Test
  public void testHashCodeWithDifferentSchemaID() {
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    SchemaEntry entry1 = new SchemaEntry(1, schemaStr);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr);
    Assert.assertEquals(entry1.hashCode(), entry2.hashCode());
  }

  @Test
  public void testHashCodeWithDifferentSpaces() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",     \"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",    \"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr2);
    Assert.assertEquals(entry1.hashCode(), entry2.hashCode());
  }

  @Test
  public void testHashCodeWithDifferentDocField() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";
    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr2);
    Assert.assertEquals(entry1.hashCode(), entry2.hashCode());
  }

  @Test
  public void testHashCodeWithDifferentSchema() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"long\"}]}";
    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(1, schemaStr2);
    Assert.assertNotEquals(entry1.hashCode(), entry2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentSchemaID() {
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    SchemaEntry entry1 = new SchemaEntry(1, schemaStr);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr);
    Assert.assertEquals(entry1, entry2);
  }

  @Test
  public void testEqualsWithDifferentDocField() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\", \"doc\": \"company name here\"}]}";
    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr2);
    Assert.assertEquals(entry1, entry2, "Two different schemas with only trivial differences should be equal.");
  }

  @Test
  public void testEqualsWithDifferentSchema() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"long\"}]}";
    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(1, schemaStr2);
    Assert.assertNotEquals(entry1, entry2);
  }

  @Test
  public void testFullCompatibilityWithSameSchema() {
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    SchemaEntry entry1 = new SchemaEntry(1, schemaStr);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr);
    Assert.assertTrue(entry1.isNewSchemaCompatible(entry2, DirectionalSchemaCompatibilityType.FULL));
  }

  @Test
  public void testCanonicalFormSchema() {
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    String schemaStr2 =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\", \"avro.java.string\" : \"string\", \"doc\":\"name field\"},{\"name\":\"company\",\"type\":\"string\"}]}";
    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr2);
    Assert.assertEquals(entry1.getCanonicalSchemaStr(), entry2.getCanonicalSchemaStr());
  }

  @Test
  public void testFullCompatibilityWithDifferentEnum() {
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

    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr2);
    Assert.assertFalse(entry1.isNewSchemaCompatible(entry2, DirectionalSchemaCompatibilityType.FULL));
  }

  @Test
  public void testFullCompatibilityWithNewField() {
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
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\", \"HEART\"]\n"
        + "                } \n" + "              },\n" + "               {\"name\": \"salary\", \"type\": \"long\"}"
        + "           ]\n" + "        }";

    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr2);
    Assert.assertFalse(entry1.isNewSchemaCompatible(entry2, DirectionalSchemaCompatibilityType.FULL));
  }

  @Test
  public void testFullCompatibilityWithNewFieldWithDefaultValue() {
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
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\", \"HEART\"]\n"
        + "                } \n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"long\", \"default\": 123 }" + "           ]\n"
        + "        }";

    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr2);
    Assert.assertTrue(entry1.isNewSchemaCompatible(entry2, DirectionalSchemaCompatibilityType.FULL));
  }

  @Test
  public void testFullCompatibilityWithFieldWithDefaultValueUpdate() {
    String schemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"float\", \"default\" : 123.0}\n" + "           ]\n"
        + "        }";
    String schemaStr2 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\", \"HEART\"]\n"
        + "                } \n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"float\", \"default\": 123 }" + "           ]\n"
        + "        }";

    SchemaEntry entry1 = new SchemaEntry(1, schemaStr1);
    SchemaEntry entry2 = new SchemaEntry(2, schemaStr2);
    Assert.assertTrue(entry1.isNewSchemaCompatible(entry2, DirectionalSchemaCompatibilityType.FULL));
    // schema equals decides whether we register a new schema or not.
    Assert.assertFalse(entry1.equals(entry2));
  }
}
