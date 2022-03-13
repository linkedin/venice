package com.linkedin.venice.schema;

import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.utils.AvroSchemaUtils;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchemaData {
  @Test
  public void testAddKeySchema() {
    String keySchemaStr = "\"string\"";
    int id = 1;
    SchemaEntry entry = new SchemaEntry(id, keySchemaStr);
    SchemaData schemaData = new SchemaData("test_store");
    schemaData.setKeySchema(entry);

    SchemaEntry keySchema = schemaData.getKeySchema();
    Assert.assertEquals(keySchema.getId(), 1);
    Assert.assertEquals(keySchemaStr, keySchema.getSchema().toString());
  }

  @Test
  public void  testSuperSetSchemaDefaultCompatibility() {
     String valueSchemaStr1 = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  " +
        "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"id\"},  " +
        "       { \"name\": \"age\", \"type\": \"float\", \"default\": -1 }" +
        "  ] " +
        " } ";

    String valueSchemaStr2 = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  " +
        "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"id\"},  " +
        "       { \"name\": \"address\", \"type\": \"string\", \"default\": \"venice\" }" +
        "  ] " +
        " } ";

   Schema newValueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr1);
   Schema existingValueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr2);
   Schema newSuperSetSchema = AvroSchemaUtils.generateSuperSetSchema(existingValueSchema, newValueSchema);
   Assert.assertTrue(new SchemaEntry(1, valueSchemaStr2).isNewSchemaCompatible(new SchemaEntry(2, newSuperSetSchema), DirectionalSchemaCompatibilityType.FULL));
  }

  @Test
  public void  testAddValueSchema() {
    String valueSchemaStr1 = "\"long\"";
    String valueSchemaStr2 = "\"string\"";

    SchemaData schemaData = new SchemaData("test_store");
    schemaData.addValueSchema(new SchemaEntry(1, valueSchemaStr1));
    schemaData.addValueSchema(new SchemaEntry(2, valueSchemaStr2));

    Assert.assertEquals(schemaData.getSchemaID(new SchemaEntry(10, valueSchemaStr1)), 1);
    Assert.assertEquals(schemaData.getSchemaID(new SchemaEntry(10, valueSchemaStr2)), 2);
    Assert.assertEquals(schemaData.getMaxValueSchemaId(), 2);
    Assert.assertEquals(new SchemaEntry(10, valueSchemaStr1), schemaData.getValueSchema(1));
  }
}
