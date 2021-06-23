package com.linkedin.venice.schema;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.Type.*;


public class TestMetadataSchemaAdapter {
  private static final Logger logger = Logger.getLogger(TestMetadataSchemaAdapter.class);


  static String primitiveTypedSchemaStr = "{\"type\": \"string\"}";
  //Expected Metadata schema for above Schema
  static String aaSchemaPrimitive = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"string_MetadataRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.venice\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\" ],\n"
      + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n"
      + "  } ]\n"
      + "}";


  static String recordSchemaStr = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"User\",\n"
      + "  \"namespace\" : \"example.avro\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"id\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"id\"\n"
      + "  }, {\n"
      + "    \"name\" : \"name\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"id\"\n"
      + "  }, {\n"
      + "    \"name\" : \"age\",\n"
      + "    \"type\" : \"int\",\n"
      + "    \"default\" : -1\n"
      + "  } ]\n"
      + "}";
  //Expected Metadata schema for above Schema
  static String aaSchemaRecord = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"User_MetadataRecord\",\n"
      + "  \"namespace\" : \"example.avro\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\", {\n"
      + "      \"type\" : \"record\",\n"
      + "      \"name\" : \"User\",\n"
      + "      \"fields\" : [ {\n"
      + "        \"name\" : \"id\",\n"
      + "        \"type\" : \"long\",\n"
      + "        \"doc\" : \"timestamp when id of the record was last updated\",\n"
      + "        \"default\" : 0\n"
      + "      }, {\n"
      + "        \"name\" : \"name\",\n"
      + "        \"type\" : \"long\",\n"
      + "        \"doc\" : \"timestamp when name of the record was last updated\",\n"
      + "        \"default\" : 0\n"
      + "      }, {\n"
      + "        \"name\" : \"age\",\n"
      + "        \"type\" : \"long\",\n"
      + "        \"doc\" : \"timestamp when age of the record was last updated\",\n"
      + "        \"default\" : 0\n"
      + "      } ]\n"
      + "    } ],\n"
      + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n"
      + "  } ]\n"
      + "}";


  static String arraySchemaStr = "{ \"type\": \"array\", \"items\": \"int\" }";
  //Expected Metadata schema for above Schema
  static String aaSchemaArray = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"array_MetadataRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.venice\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\" ],\n"
      + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n"
      + "  } ]\n"
      + "}";


  static String mapSchemaStr = "{ \"type\": \"map\", \"values\": \"int\" }";
  //Expected Metadata schema for above Schema
  static String aaSchemaMap = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"map_MetadataRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.venice\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\" ],\n"
      + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n"
      + "  } ]\n"
      + "}";


  static String unionSchemaStr = "[ {\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"namerecord\",\n"
      + "  \"namespace\" : \"example.avro\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"firstname\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"\"\n"
      + "  }, {\n"
      + "    \"name\" : \"lastname\",\n"
      + "    \"type\" : \"string\",\n"
      + "    \"default\" : \"\"\n"
      + "  } ]\n"
      + "}, {\n"
      + "  \"type\" : \"array\",\n"
      + "  \"items\" : \"int\"\n"
      + "}, {\n"
      + "  \"type\" : \"map\",\n"
      + "  \"values\" : \"int\"\n"
      + "}, \"string\" ]";
  //Expected Metadata schema for above Schema
  static String aaSchemaUnion = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"union_MetadataRecord\",\n"
      + "  \"namespace\" : \"com.linkedin.venice\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"timestamp\",\n"
      + "    \"type\" : [ \"long\" ],\n"
      + "    \"doc\" : \"timestamp when the full record was last updated\",\n"
      + "    \"default\" : 0\n"
      + "  } ]\n"
      + "}";


  @Test
  public void testMetadataSchemaForPrimitive() {
    Schema origSchema = Schema.create(INT);
    Schema aaSchema = MetadataSchemaAdapter.parse(origSchema, 1);
    String aaSchemaStr = aaSchema.toString(true);
    logger.info(aaSchemaStr);

    verifyFullUpdateTsRecordPresent(aaSchema, true);
  }

  @Test
  public void testMetadataSchemaForPrimitiveTyped() {
    Schema origSchema = Schema.parse(primitiveTypedSchemaStr);
    Schema aaSchema = MetadataSchemaAdapter.parse(origSchema, 1);
    String aaSchemaStr = aaSchema.toString(true);
    logger.info(aaSchemaStr);
    Assert.assertEquals(aaSchema, Schema.parse(aaSchema.toString()));
    verifyFullUpdateTsRecordPresent(aaSchema, true);

  }

  @Test
  public void testMetadataSchemaForRecord() {
    Schema origSchema = Schema.parse(recordSchemaStr);
    String OrigSchemaStr = origSchema.toString(true);
    Schema aaSchema = MetadataSchemaAdapter.parse(origSchema, 1);
    String aaSchemaStr = aaSchema.toString(true);
    logger.info(OrigSchemaStr);
    logger.info(aaSchemaStr);

    Assert.assertEquals(aaSchema, Schema.parse(aaSchema.toString()));
    verifyFullUpdateTsRecordPresent(aaSchema, false);
    Schema recordTsSchema = aaSchema.getField("timestamp").schema().getTypes().get(1);
    Assert.assertEquals(recordTsSchema.getType(), RECORD);
    Assert.assertEquals(recordTsSchema.getFields().size(), 3);
    Assert.assertEquals(recordTsSchema.getField("id").schema().getType(), LONG);
    Assert.assertEquals(recordTsSchema.getField("name").schema().getType(), LONG);
    Assert.assertEquals(recordTsSchema.getField("age").schema().getType(), LONG);
    Assert.assertEquals(recordTsSchema.getField("id").defaultValue().asLong(), 0);
    Assert.assertEquals(recordTsSchema.getField("name").defaultValue().asLong(), 0);
    Assert.assertEquals(recordTsSchema.getField("age").defaultValue().asLong(), 0);


  }

  @Test
  public void testMetadataSchemaForArray() {
    Schema origSchema = Schema.parse(arraySchemaStr);
    String OrigSchemaStr = origSchema.toString(true);
    Schema aaSchema = MetadataSchemaAdapter.parse(origSchema, 1);
    String aaSchemaStr = aaSchema.toString(true);
    logger.info(OrigSchemaStr);
    logger.info(aaSchemaStr);
    verifyFullUpdateTsRecordPresent(aaSchema, true);


  }

  @Test
  public void testMetadataSchemaForMap() {
    Schema origSchema = Schema.parse(mapSchemaStr);
    String OrigSchemaStr = origSchema.toString(true);
    Schema aaSchema = MetadataSchemaAdapter.parse(origSchema, 1);
    String aaSchemaStr = aaSchema.toString(true);
    logger.info(OrigSchemaStr);
    logger.info(aaSchemaStr);
    verifyFullUpdateTsRecordPresent(aaSchema, true);


  }

  @Test
  public void testMetadataSchemaForUnion() {
    Schema origSchema = Schema.parse(unionSchemaStr);
    String OrigSchemaStr = origSchema.toString(true);
    Schema aaSchema = MetadataSchemaAdapter.parse(origSchema, 1);
    String aaSchemaStr = aaSchema.toString(true);
    logger.info(OrigSchemaStr);
    logger.info(aaSchemaStr);

    verifyFullUpdateTsRecordPresent(aaSchema, true);
  }

  private void verifyFullUpdateTsRecordPresent(Schema aaSchema, boolean onlyRootTsPresent) {
    Assert.assertEquals(aaSchema.getType(), RECORD);
    Assert.assertEquals(aaSchema.getFields().size(), 1);
    Schema.Field tsField = aaSchema.getField("timestamp");
    Assert.assertEquals(tsField.schema().getType(), UNION);
    Assert.assertEquals(tsField.defaultValue().asLong(), 0);

    if (onlyRootTsPresent) {
      Assert.assertEquals(tsField.schema().getTypes().size(), 1);
      Assert.assertEquals(tsField.schema().getTypes().get(0).getType(), LONG);
    } else {
      Assert.assertEquals(tsField.schema().getTypes().size(), 2);
      Assert.assertEquals(tsField.schema().getTypes().get(0).getType(), LONG);
      Assert.assertEquals(tsField.schema().getTypes().get(1).getType(), RECORD);
    }
  }

}