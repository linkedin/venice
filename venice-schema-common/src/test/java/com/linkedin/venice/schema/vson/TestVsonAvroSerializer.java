package com.linkedin.venice.schema.vson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVsonAvroSerializer {
  @Test
  public void testCanSerializePrimitiveSchema() {
    VsonAvroSerializer serializer;

    String integerSchema = "\"int32\"";
   serializer = VsonAvroSerializer.fromSchemaStr(integerSchema);

    byte[] bytes = serializer.toBytes(123);
    Assert.assertEquals(123, serializer.toObject(bytes));
    Assert.assertEquals(123, serializer.bytesToAvro(bytes));

    System.out.println(VsonAvroSchemaAdapter.parse("\"int32\""));
    System.out.println(VsonAvroSchemaAdapter.parse("[{\"member_id\":\"int32\", \"score\":\"float32\"}]"));
  }

  @Test
  public void testCanSerializeComplexSchema() {
    String recordSchema = "[{\"email\":\"string\", \"score\":\"float32\"}]";
    VsonAvroSerializer serializer = VsonAvroSerializer.fromSchemaStr(recordSchema);

    //setup Vson record
    List<Map<String, Object>> record = new ArrayList<>();
    Map<String, Object> recordContent = new HashMap<>();
    recordContent.put("email", "abc");
    recordContent.put("score", 1f);
    record.add(recordContent);
    byte[] bytes = serializer.toBytes(record);

    //bytes to Vson object
    Object deserializedVsonRecord = serializer.toObject(bytes);
    Assert.assertTrue(deserializedVsonRecord instanceof List);

    Object deserializedVsonContent = ((List) deserializedVsonRecord).get(0);
    Assert.assertTrue(deserializedVsonContent instanceof Map);
    Assert.assertEquals(((Map) deserializedVsonContent).get("email"), "abc");
    Assert.assertEquals(((Map) deserializedVsonContent).get("score"),1f);

    //bytes to Avro object
    Object deserializedAvroRecord = serializer.bytesToAvro(bytes);
    Assert.assertTrue(deserializedAvroRecord instanceof GenericData.Array);

    Object deserializedAvroContent = ((List) deserializedAvroRecord).get(0);
    Assert.assertTrue(deserializedAvroContent instanceof GenericData.Record);
    Assert.assertEquals(((GenericData.Record) deserializedAvroContent).get("email"), new Utf8("abc"));
    Assert.assertEquals(((GenericData.Record) deserializedAvroContent).get("score"), 1f);
  }
}