package com.linkedin.venice.schema.vson;

import static com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter.stripFromUnion;

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

    Schema avroByte = readStrToAvroSchema("\"int8\"");
    Assert.assertEquals(avroByte.getType(), Schema.Type.FIXED);
    Assert.assertEquals(avroByte.getFixedSize(), 1);

    Schema avroShort = readStrToAvroSchema("\"int16\"");
    Assert.assertEquals(avroShort.getType(), Schema.Type.FIXED);
    Assert.assertEquals(avroShort.getFixedSize(), 2);

    Assert.assertEquals(readStrToVsonSchema("\"int32\"").getType(), VsonTypes.INT32);
    Assert.assertEquals(readStrToVsonSchema("\"float32\"").getType(), VsonTypes.FLOAT32);
    Assert.assertEquals(readStrToVsonSchema("\"float64\"").getType(), VsonTypes.FLOAT64);
    Assert.assertEquals(readStrToVsonSchema("\"string\"").getType(), VsonTypes.STRING);
    Assert.assertEquals(readStrToVsonSchema("\"int8\"").getType(), VsonTypes.INT8);
    Assert.assertEquals(readStrToVsonSchema("\"int16\"").getType(), VsonTypes.INT16);
  }

  @Test
  public void adapterCanReadListAndMap() {
    String listStr =
        "[{\"email\":\"string\", \"metadata\":[{\"key\":\"string\", \"value\":\"string\"}], \"score\":\"float32\"}]";

    Schema avroSchema = readStrToAvroSchema(listStr);

    Assert.assertEquals((avroSchema).getType(), Schema.Type.ARRAY);
    Assert.assertEquals(avroSchema.getElementType().getType(), Schema.Type.UNION);

    Schema strippedElementSchema = stripFromUnion(avroSchema.getElementType());
    Assert.assertEquals(strippedElementSchema.getType(), Schema.Type.RECORD);
    Assert.assertEquals(stripFromUnion(strippedElementSchema.getField("email").schema()).getType(), Schema.Type.STRING);
    Assert
        .assertEquals(stripFromUnion(strippedElementSchema.getField("metadata").schema()).getType(), Schema.Type.ARRAY);
    Assert.assertEquals(stripFromUnion(strippedElementSchema.getField("score").schema()).getType(), Schema.Type.FLOAT);

    VsonSchema vsonSchema = readStrToVsonSchema(listStr);

    Assert.assertTrue(vsonSchema.getType() instanceof List);
    Assert.assertTrue(((List) vsonSchema.getType()).get(0) instanceof Map);

    Map<String, Object> vsonRecord = (Map) ((List) vsonSchema.getType()).get(0);
    Assert.assertEquals(vsonRecord.get("email"), VsonTypes.STRING);
    Assert.assertTrue(vsonRecord.get("metadata") instanceof List);
    Assert.assertEquals(vsonRecord.get("score"), VsonTypes.FLOAT32);
  }

  @Test
  public void adapterThrowExceptionForInvalidStr() {
    try {
      readStrToAvroSchema("random string");
      Assert.fail();
    } catch (VsonSerializationException e) {
    }

    try {
      readStrToVsonSchema("123456");
      Assert.fail();
    } catch (VsonSerializationException e) {
    }
  }

  private Schema readStrToAvroSchema(String schemaStr) {
    return stripFromUnion(VsonAvroSchemaAdapter.parse(schemaStr));
  }

  private VsonSchema readStrToVsonSchema(String schemaStr) {
    return VsonSchema.parse(schemaStr);
  }
}
