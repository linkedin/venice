package com.linkedin.venice.schema;

import com.linkedin.venice.schema.avro.WriteComputeSchemaAdapter;
import com.linkedin.venice.utils.TestPushUtils;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.Type.*;


public class TestWriteComputeSchemaAdapter {
  private String recordSchemaStr = TestPushUtils.USER_SCHEMA_STRING;
  private String recordOfArraySchemaStr =
      "{\n" +
      "  \"type\" : \"record\",\n" +
      "  \"name\" : \"testRecord\",\n" +
      "  \"namespace\" : \"avro.example\",\n" +
      "  \"fields\" : [ {\n" + "    \"name\" : \"intArray\",\n" +
      "    \"type\" : {\n" +
      "      \"type\" : \"array\",\n" +
      "      \"items\" : \"int\"\n" +
      "    }\n" + "  }, {\n" +
      "    \"name\" : \"floatArray\",\n" + "    \"type\" : {\n" +
      "      \"type\" : \"array\",\n" +
      "      \"items\" : \"float\"\n" + "    }\n" +
      "  } ]\n" +
      "}";

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
    Schema recordWriteSchema = WriteComputeSchemaAdapter.parse(recordSchemaStr);
    Assert.assertEquals(recordWriteSchema.getType(), RECORD);
    Assert.assertEquals(recordWriteSchema.getFields().size(), 3);
    Assert.assertEquals(recordWriteSchema.getField("age").schema().getType(), UNION);
    Assert.assertEquals(recordWriteSchema.getField("age").schema().getTypes().get(1), Schema.create(INT));
    Assert.assertEquals(recordWriteSchema.getField("id").schema().getTypes().get(1), Schema.create(STRING));

    //test parsing record of arrays
    Schema recordOfArraysWriteSchema = WriteComputeSchemaAdapter.parse(Schema.parse(recordOfArraySchemaStr));
    Schema intArrayFieldWriteSchema = recordOfArraysWriteSchema.getField("intArray").schema();
    Assert.assertEquals(intArrayFieldWriteSchema.getTypes().get(1).getNamespace(), "avro.example");

    Schema floatArrayFieldWriteSchema = recordOfArraysWriteSchema.getField("floatArray").schema();
    Assert.assertEquals(floatArrayFieldWriteSchema.getTypes().get(1).getNamespace(), "avro.example");
  }
}