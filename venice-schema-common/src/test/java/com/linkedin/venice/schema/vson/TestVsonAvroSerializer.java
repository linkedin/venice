package com.linkedin.venice.schema.vson;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVsonAvroSerializer {
  private void testSerializer(String vsonSchemaStr, Supplier valueSupplier) {
    testSerializer(vsonSchemaStr, valueSupplier,
        (serializer, bytes) -> {
          Assert.assertEquals(serializer.toObject(bytes), valueSupplier.get());
          Assert.assertEquals(serializer.bytesToAvro(bytes), valueSupplier.get());
        });
    testSerializerWithNullValue(vsonSchemaStr);
  }

  private void testSerializerWithNullValue(String vsonSchemaStr) {
    testSerializer(vsonSchemaStr, () -> null,
        (serializer, bytes) -> {
          Assert.assertEquals(serializer.toObject(bytes), null);
          Assert.assertEquals(serializer.bytesToAvro(bytes), null);
        });
  }

  private void testSerializer(String vsonSchemaStr, Supplier valueSupplier, ValueValidator valueValidator) {
    VsonAvroSerializer serializer = VsonAvroSerializer.fromSchemaStr(vsonSchemaStr);
    byte[] bytes = serializer.toBytes(valueSupplier.get());
    valueValidator.validate(serializer, bytes);
  }

  @Test
  public void testCanSerializePrimitiveSchema() throws IOException {
    testSerializer("\"int32\"", () -> 123);
    testSerializer("\"int64\"", () -> 123l);
    testSerializer("\"float32\"", () -> 123f);
    testSerializer("\"float64\"", () -> 123d);
    testSerializer("\"boolean\"", () -> true);

    //test String. 'string' is stored as Utf in Avro
    testSerializer("\"string\"", () -> "123",
        (serializer, bytes) -> {
          Assert.assertEquals(serializer.bytesToAvro(bytes), new Utf8("123"));
          Assert.assertEquals(serializer.toObject(bytes), "123");
        });
    testSerializerWithNullValue("\"string\"");

    //test byte. 'bytes' is stored as ByteBuffer in Avro
    byte[] randomBytes = new byte[10];
    new Random().nextBytes(randomBytes);
    testSerializer("\"bytes\"", () -> randomBytes,
        (serializer, bytes) -> {
          Assert.assertEquals(serializer.toObject(bytes), randomBytes);
          Assert.assertEquals(serializer.bytesToAvro(bytes), ByteBuffer.wrap(randomBytes));
        });
    testSerializerWithNullValue("\"bytes\"");
  }

  @Test
  public void testCanSerializeComplexSchema() {
    String complexSchemaStr = "[{\"email\":\"string\", \"score\":\"float32\"}]";
    List<Map<String, Object>> record = new ArrayList<>();
    Map<String, Object> recordContent = new HashMap<>();
    recordContent.put("email", "abc");
    recordContent.put("score", 1f);
    record.add(recordContent);

    testSerializer(complexSchemaStr, () -> record, (serializer, bytes) -> {
      Object vsonRecord = serializer.toObject(bytes);
      Assert.assertTrue(vsonRecord instanceof List);
      Object vsonRecordContent = ((List) vsonRecord).get(0);
      Assert.assertTrue(vsonRecordContent instanceof Map);
      Assert.assertEquals(((Map) vsonRecordContent).get("email"), "abc");
      Assert.assertEquals(((Map) vsonRecordContent).get("score"), 1f);

      Object avroRecord = serializer.bytesToAvro(bytes);
      Assert.assertTrue(avroRecord instanceof  GenericData.Array);
      Object avroRecordContent = ((List) avroRecord).get(0);
      Assert.assertTrue(avroRecordContent instanceof GenericData.Record);
      Assert.assertEquals(((GenericData.Record) avroRecordContent).get("email"), new Utf8("abc"));
      Assert.assertEquals(((GenericData.Record) avroRecordContent).get("score"), 1f);
    });

    //test record with null field
    recordContent.put("score", null);

    testSerializer(complexSchemaStr, () -> record, (serializer, bytes) -> {
      Object vsonRecord = ((List) serializer.toObject(bytes)).get(0);
      Assert.assertEquals(((Map) vsonRecord).get("email"), "abc");
      Assert.assertEquals(((Map) vsonRecord).get("score"), null);

      Object avroRecord = ((GenericData.Array) serializer.bytesToAvro(bytes)).get(0);
      Assert.assertEquals(((GenericData.Record) avroRecord).get("email"), new Utf8("abc"));
      Assert.assertEquals(((GenericData.Record) avroRecord).get("score"), null);
    });

    //test list with null element
    record.add(null);
    testSerializer(complexSchemaStr, () -> record, (serializer, bytes) -> {
      Object vsonRecord = ((List) serializer.toObject(bytes)).get(1);
      Assert.assertEquals(vsonRecord, null);

      Object avroRecord = ((List) serializer.bytesToAvro(bytes)).get(1);
      Assert.assertEquals(avroRecord, null);
    });

    testSerializerWithNullValue(complexSchemaStr);
  }

  private interface ValueValidator {
    void validate(VsonAvroSerializer serializer, byte[] bytes);
  }
}