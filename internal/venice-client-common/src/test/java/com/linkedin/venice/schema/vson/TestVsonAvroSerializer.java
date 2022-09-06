package com.linkedin.venice.schema.vson;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVsonAvroSerializer {
  private void testSerializer(String vsonSchemaStr, Supplier valueSupplier) {
    testSerializer(vsonSchemaStr, valueSupplier, (serializer, bytes) -> {
      Assert.assertEquals(serializer.toObject(bytes), valueSupplier.get());
      Assert.assertEquals(serializer.bytesToAvro(bytes), valueSupplier.get());
    });
    testSerializerWithNullValue(vsonSchemaStr);
  }

  private void testSerializerWithNullValue(String vsonSchemaStr) {
    testSerializer(vsonSchemaStr, () -> null, (serializer, bytes) -> {
      Assert.assertNull(serializer.toObject(bytes));
      Assert.assertNull(serializer.bytesToAvro(bytes));
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

    // test String. 'string' is stored as Utf in Avro
    testSerializer("\"string\"", () -> "123", (serializer, bytes) -> {
      Assert.assertEquals(serializer.bytesToAvro(bytes), new Utf8("123"));
      Assert.assertEquals(serializer.toObject(bytes), "123");
    });
    testSerializerWithNullValue("\"string\"");

    // test bytes. 'bytes' is stored as ByteBuffer in Avro
    byte[] randomBytes = new byte[10];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    testSerializer("\"bytes\"", () -> randomBytes, (serializer, bytes) -> {
      Assert.assertEquals(serializer.toObject(bytes), randomBytes);
      Assert.assertEquals(serializer.bytesToAvro(bytes), ByteBuffer.wrap(randomBytes));
    });
    testSerializerWithNullValue("\"bytes\"");

    // test single byte. 'byte' is represented as 'int8' in Vson
    // and it is represented as Fixed (with 1 length) in Avro
    byte byte_val = 40;
    testSerializer("\"int8\"", () -> byte_val, (serializer, bytes) -> {
      Assert.assertEquals(serializer.toObject(bytes), byte_val);
      Object avroByte = serializer.bytesToAvro(bytes);
      Assert.assertTrue(avroByte instanceof GenericData.Fixed);
      byte[] avroByteArray = ((GenericData.Fixed) avroByte).bytes();
      Assert.assertEquals(avroByteArray.length, 1);
      Assert.assertEquals(avroByteArray[0], byte_val);
    });
    testSerializerWithNullValue("\"int8\"");

    // test short. 'short' is represented as 'int16' in Vson
    // and it is represented as Fixed (with 2 length) in Avro
    testSerializer("\"int16\"", () -> (short) -2, (serializer, bytes) -> {
      Assert.assertEquals(serializer.toObject(bytes), (short) -2);
      Object avroShort = serializer.bytesToAvro(bytes);
      Assert.assertTrue(avroShort instanceof GenericData.Fixed);
      byte[] avroByteArray = ((GenericData.Fixed) avroShort).bytes();
      Assert.assertEquals(avroByteArray.length, 2);
      // -2 is equal to 11111110 (0xFFFE)
      Assert.assertEquals(avroByteArray[0], (byte) 0xFF);
      Assert.assertEquals(avroByteArray[1], (byte) 0xFE);
    });
    testSerializerWithNullValue("\"int16\"");
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
      Assert.assertTrue(avroRecord instanceof GenericData.Array);
      Object avroRecordContent = ((List) avroRecord).get(0);
      Assert.assertTrue(avroRecordContent instanceof GenericData.Record);
      Assert.assertEquals(((GenericData.Record) avroRecordContent).get("email"), new Utf8("abc"));
      Assert.assertEquals(((GenericData.Record) avroRecordContent).get("score"), 1f);
    });

    // test record with null field
    recordContent.put("score", null);

    testSerializer(complexSchemaStr, () -> record, (serializer, bytes) -> {
      Object vsonRecord = ((List) serializer.toObject(bytes)).get(0);
      Assert.assertEquals(((Map) vsonRecord).get("email"), "abc");
      Assert.assertNull(((Map) vsonRecord).get("score"));

      Object avroRecord = ((GenericData.Array) serializer.bytesToAvro(bytes)).get(0);
      Assert.assertEquals(((GenericData.Record) avroRecord).get("email"), new Utf8("abc"));
      Assert.assertNull(((GenericData.Record) avroRecord).get("score"));
    });

    // test list with null element
    record.add(null);
    testSerializer(complexSchemaStr, () -> record, (serializer, bytes) -> {
      Object vsonRecord = ((List) serializer.toObject(bytes)).get(1);
      Assert.assertNull(vsonRecord);

      Object avroRecord = ((List) serializer.bytesToAvro(bytes)).get(1);
      Assert.assertNull(avroRecord);
    });

    testSerializerWithNullValue(complexSchemaStr);
  }

  private interface ValueValidator {
    void validate(VsonAvroSerializer serializer, byte[] bytes);
  }
}
