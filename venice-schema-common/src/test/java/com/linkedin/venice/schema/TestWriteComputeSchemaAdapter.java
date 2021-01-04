package com.linkedin.venice.schema;

import java.util.Arrays;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.Type.*;


public class TestWriteComputeSchemaAdapter {
  private String recordSchemaStr = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"User\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  " +
      "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"id\"},  " +
      "       { \"name\": \"age\", \"type\": \"int\", \"default\": -1 }" +
      "  ] " +
      " } ";

  private String recordOfArraySchemaStr = "{\n"
      +"  \"type\" : \"record\",\n"
      +"  \"name\" : \"testRecord\",\n"
      +"  \"namespace\" : \"avro.example\",\n"
      +"  \"fields\" : [ {\n"
      +"    \"name\" : \"intArray\",\n"
      +"    \"type\" : {\n"
      +"      \"type\" : \"array\",\n"
      +"      \"items\" : \"int\"\n"
      +"    },\n"
      +"    \"default\" : [ ]\n"
      +"  }, {\n"
      +"    \"name\" : \"floatArray\",\n"
      +"    \"type\" : {\n"
      +"      \"type\" : \"array\",\n"
      +"      \"items\" : \"float\"\n"
      +"    },\n"
      +"    \"default\" : [ ]\n"
      +"  } ]\n"
      +"}";

  private String recordOfUnionWithCollectionStr = "{\n"
      + "  \"type\": \"record\",\n" + "  \"name\": \"testRecord\",\n"
      + "  \"namespace\": \"avro.example\",\n"
      + "  \"fields\": [\n" + "    {\n"
      + "      \"name\": \"intArray\",\n"
      + "      \"type\":[\n"
      + "      {\n"
      + "        \"type\": \"array\",\n"
      + "        \"items\": \"int\"\n"
      + "      },\n"
      + "      \"boolean\"\n"
      + "      ],\n"
      + "      \"default\": [\n"
      + "      ]\n"
      + "    },\n"
      + "    {\n"
      + "      \"name\": \"floatArray\",\n"
      + "      \"type\": {\n"
      + "        \"type\": \"array\",\n"
      + "        \"items\": \"float\"\n"
      + "      },\n"
      + "      \"default\": [\n"
      + "        \n"
      + "      ]\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  private String recordOfUnionWithTwoCollectionsStr = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"testRecord\",\n"
      + "  \"namespace\": \"avro.example\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"intArray\",\n"
      + "      \"type\":[\n"
      + "      {\n"
      + "        \"type\": \"array\",\n"
      + "        \"items\": \"int\"\n"
      + "      },\n"
      + "      {\n"
      + "        \"type\": \"map\",\n"
      + "        \"values\": \"long\"\n"
      + "      },\n"
      + "      \"boolean\"\n"
      + "      ],\n"
      + "      \"default\": [\n"
      + "      ]\n"
      + "    },\n"
      + "    {\n"
      + "      \"name\": \"floatArray\",\n"
      + "      \"type\": {\n"
      + "        \"type\": \"array\",\n"
      + "        \"items\": \"float\"\n"
      + "      },\n"
      + "      \"default\": [\n"
      + "        \n"
      + "      ]\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  private String recordOfNullableArrayStr = "{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"testRecord\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"nullableArrayField\",\n"
      + "    \"type\" : [ \"null\", {\n"
      + "      \"type\" : \"array\",\n"
      + "      \"items\" : {\n"
      + "        \"type\" : \"record\",\n"
      + "        \"name\" : \"simpleRecord\",\n"
      + "        \"fields\" : [ {\n"
      + "          \"name\" : \"intField\",\n"
      + "          \"type\" : \"int\",\n"
      + "          \"default\" : 0\n"
      + "        } ]\n"
      + "      }\n"
      + "    } ],\n"
      + "    \"default\" : null\n"
      + "  } ]\n"
      + "}";


  @Test
  public void testAdapterCanParseBasicSchema() {
    //For primitive, union, fixed type, the writeComputeSchema looks the same as its original one.
    Assert.assertEquals(Schema.create(INT), WriteComputeSchemaAdapter.parse(Schema.create(INT)));
    Assert.assertEquals(Schema.create(FLOAT), WriteComputeSchemaAdapter.parse(Schema.create(FLOAT)));

    Schema unionSchema = Schema.createUnion(Arrays.asList(Schema.create(INT)));
    Assert.assertEquals(unionSchema, WriteComputeSchemaAdapter.parse(unionSchema));

    Schema fixedSchema = Schema.createFixed("FixedSchema", null, null, 1);
    Assert.assertEquals(fixedSchema, WriteComputeSchemaAdapter.parse(fixedSchema));
  }

  @Test
  public void testAdapterCanParseListSchema() {
    Schema arrayWriteSchema = WriteComputeSchemaAdapter.parse(Schema.createArray(Schema.create(INT)));
    Assert.assertEquals(arrayWriteSchema.getType(), UNION);
    Assert.assertEquals(arrayWriteSchema.getTypes().size(), 2);

    Assert.assertEquals(arrayWriteSchema.getTypes().get(0).getType(), RECORD);
    Schema setUnionSchema = arrayWriteSchema.getTypes().get(0).getField("setUnion").schema();
    Assert.assertEquals(setUnionSchema, Schema.createArray(Schema.create(INT)));

    Assert.assertEquals(arrayWriteSchema.getTypes().get(1), Schema.createArray(Schema.create(INT)));
  }

  @Test
  public void testAdapterCanParseMapSchema() {
    Schema mapWriteSchema = WriteComputeSchemaAdapter.parse(Schema.createMap(Schema.create(FLOAT)));
    Assert.assertEquals(mapWriteSchema.getType(), UNION);
    Assert.assertEquals(mapWriteSchema.getTypes().size(), 2);

    Assert.assertEquals(mapWriteSchema.getTypes().get(0).getType(), RECORD);
    Schema mapDiffSchema = mapWriteSchema.getTypes().get(0).getField("mapDiff").schema();
    Assert.assertEquals(mapDiffSchema.getType(), ARRAY);
    Assert.assertEquals(mapDiffSchema, Schema.createArray(Schema.create(STRING)));

    Assert.assertEquals(mapWriteSchema.getTypes().get(1), Schema.createMap(Schema.create(FLOAT)));
  }

  @Test
  public void testAdapterCanParseRecordSchema() {
    //test parsing record
    Schema recordWriteSchema = WriteComputeSchemaAdapter.parse(recordSchemaStr).getTypes().get(0);
    Assert.assertEquals(recordWriteSchema.getType(), RECORD);
    Assert.assertEquals(recordWriteSchema.getFields().size(), 3);
    Assert.assertEquals(recordWriteSchema.getField("age").schema().getType(), UNION);
    Assert.assertEquals(recordWriteSchema.getField("age").schema().getTypes().get(1), Schema.create(INT));
    Assert.assertEquals(recordWriteSchema.getField("id").schema().getTypes().get(1), Schema.create(STRING));

    //test parsing record of arrays
    Schema recordOfArraysWriteSchema = WriteComputeSchemaAdapter.parse(Schema.parse(recordOfArraySchemaStr)).getTypes().get(0);
    Schema intArrayFieldWriteSchema = recordOfArraysWriteSchema.getField("intArray").schema();
    Assert.assertEquals(intArrayFieldWriteSchema.getTypes().get(1).getNamespace(), "avro.example");

    Schema floatArrayFieldWriteSchema = recordOfArraysWriteSchema.getField("floatArray").schema();
    Assert.assertEquals(floatArrayFieldWriteSchema.getTypes().get(1).getNamespace(), "avro.example");
  }

  @Test
  public void testAdapterCanParseRecordSchemaWithUnion() {
    //test parsing a schema with a union type that contains 1 collection
    Schema recordWriteSchema = WriteComputeSchemaAdapter.parse(recordOfUnionWithCollectionStr).getTypes().get(0);
    Assert.assertEquals(recordWriteSchema.getType(), RECORD);
    Assert.assertEquals(recordWriteSchema.getFields().size(), 2);
    Assert.assertEquals(recordWriteSchema.getField("intArray").schema().getType(), UNION);
    // Check for NoOp option
    Assert.assertEquals(recordWriteSchema.getField("intArray").schema().getTypes().get(0).getType(), RECORD);
    Assert.assertEquals(recordWriteSchema.getField("intArray").schema().getTypes().get(0).getName(), "NoOp");
    Assert.assertEquals(recordWriteSchema.getField("intArray").schema().getTypes().get(1), Schema.createArray(Schema.create(INT)));
    Assert.assertEquals(recordWriteSchema.getField("intArray").schema().getTypes().get(2), Schema.create(BOOLEAN));

  }

  @Test
  public void testAdapterCanNotParseRecordWithUnionOfMultipleCollections() {
    //test parsing a schema with a union type that contains 2 collections (should barf)
    Assert.assertThrows(() -> WriteComputeSchemaAdapter.parse(recordOfUnionWithTwoCollectionsStr));
  }

  @Test
  public void testAdapterCanParseNullableField() {
    //test parsing nullable array field. The parser is supposed to dig into the union and
    //parse the array
    Schema nullableRecordWriteSchema = WriteComputeSchemaAdapter.parse(recordOfNullableArrayStr).getTypes().get(0);
    Assert.assertEquals(nullableRecordWriteSchema.getType(), RECORD);

    //Check the elements inside the union
    Schema writeComputeFieldSchema = nullableRecordWriteSchema.getField("nullableArrayField").schema();
    Assert.assertEquals(writeComputeFieldSchema.getType(), UNION);
    Assert.assertEquals(writeComputeFieldSchema.getTypes().size(), 4);
    Assert.assertEquals(writeComputeFieldSchema.getTypes().get(0).getName(), "NoOp");
    Assert.assertEquals(writeComputeFieldSchema.getTypes().get(1).getType(), NULL);
    Assert.assertEquals(writeComputeFieldSchema.getTypes().get(2).getName(), "nullableArrayFieldListOps");
    Assert.assertEquals(writeComputeFieldSchema.getTypes().get(3).getType(), ARRAY);
  }
}