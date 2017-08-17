package com.linkedin.venice.schema.vson;

import com.linkedin.venice.serializer.VsonSerializationException;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVsonSchemaAdapter {
  @Test
  public void adapterCanReadPrimitiveType() {
    Assert.assertEquals(readStrToAvroSchema("\"int32\"").getType(), Schema.Type.INT);
    Assert.assertEquals(readStrToAvroSchema("\"float32\"").getType(), Schema.Type.FLOAT);
    Assert.assertEquals(readStrToAvroSchema("\"float64\"").getType(), Schema.Type.DOUBLE);
    Assert.assertEquals(readStrToAvroSchema("\"string\"").getType(), Schema.Type.STRING);

    Assert.assertEquals(readStrToVsonSchema("\"int32\"").getType(), VsonTypes.INT32);
    Assert.assertEquals(readStrToVsonSchema("\"float32\"").getType(), VsonTypes.FLOAT32);
    Assert.assertEquals(readStrToVsonSchema("\"float64\"").getType(), VsonTypes.FLOAT64);
    Assert.assertEquals(readStrToVsonSchema("\"string\"").getType(), VsonTypes.STRING);

  }

  @Test
  public void adapterCanReadListAndMap() {
    String listStr = "[{\"email\":\"string\", \"metadata\":[{\"key\":\"string\", \"value\":\"string\"}], \"score\":\"float32\"}]";

    Schema avroSchema = readStrToAvroSchema(listStr);

    Assert.assertEquals(avroSchema.getType(), Schema.Type.ARRAY);
    Assert.assertEquals(avroSchema.getElementType().getType(), Schema.Type.RECORD);

    Assert.assertEquals(avroSchema.getElementType().getField("email").schema().getType(), Schema.Type.STRING);
    Assert.assertEquals(avroSchema.getElementType().getField("metadata").schema().getType(), Schema.Type.ARRAY);
    Assert.assertEquals(avroSchema.getElementType().getField("score").schema().getType(), Schema.Type.FLOAT);

    VsonSchema vsonSchema = readStrToVsonSchema(listStr);

    Assert.assertTrue(vsonSchema.getType() instanceof List);
    Assert.assertTrue(((List)vsonSchema.getType()).get(0) instanceof Map);

    Map<String, Object> vsonRecord = (Map)((List)vsonSchema.getType()).get(0);
    Assert.assertEquals(vsonRecord.get("email"), VsonTypes.STRING);
    Assert.assertTrue(vsonRecord.get("metadata") instanceof List);
    Assert.assertEquals(vsonRecord.get("score"), VsonTypes.FLOAT32);
  }

  @Test
  public void adapterThrowExceptionForInvalidStr() {
    try {
      readStrToAvroSchema("random string");
      Assert.fail();
    } catch (VsonSerializationException e) {}

    try {
      readStrToVsonSchema("123456");
      Assert.fail();
    } catch (VsonSerializationException e) {}
  }

  private Schema readStrToAvroSchema(String schemaStr) {
    return VsonAvroSchemaAdapter.parse(schemaStr);
  }

  private VsonSchema readStrToVsonSchema(String schemaStr) {
    return VsonSchema.fromJson(schemaStr);
  }
}

