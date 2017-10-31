package com.linkedin.venice.schema.vson;

import com.linkedin.venice.serializer.AvroGenericSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter.stripFromUnion;

public class TestVsonAvroDatumReader {
  @Test
  public void testReaderCanReadPrimitive() throws IOException {
    testReader("\"int32\"", () -> 123);
    testReader("\"int64\"", () -> 123l);
    testReader("\"float32\"", () -> 123f);
    testReader("\"float64\"", () -> 123d);
    testReader("\"boolean\"", () -> true);
    testReader("\"string\"", () -> "123");

    String byteSchema = "\"bytes\"";
    byte[] randomBytes = new byte[10];
    new Random().nextBytes(randomBytes);
    testReader(byteSchema, () -> ByteBuffer.wrap(randomBytes),
        (vsonObject) -> Assert.assertEquals(vsonObject, randomBytes));
    testReadNullValue(byteSchema);

    //single byte
    byte[] singleByteArray = {randomBytes[0]};
    GenericData.Fixed fixedByte = new GenericData.Fixed(singleByteArray);
    testReader("\"int8\"", () -> fixedByte,
        vsonObject -> Assert.assertEquals(vsonObject, randomBytes[0]));
    testReadNullValue("\"int8\"");

    //short
    byte[] shortBytesArray = {(byte) 0xFF, (byte) 0xFE}; //0xFFFE = -2
    GenericData.Fixed fixedShort = new GenericData.Fixed(shortBytesArray);
    testReader("\"int16\"", () -> fixedShort,
        vsonObject -> Assert.assertEquals(vsonObject, (short) -2));
    testReadNullValue("\"int16\"");
  }

  @Test
  public void testReaderCanReadRecord() throws IOException {
    String vsonSchemaStr = "{\"member_id\":\"int32\", \"score\":\"float32\"}";

    //strip the top level union as this is intended for constructing Avro GenericData.Record
    Schema avroSchema = stripFromUnion(VsonAvroSchemaAdapter.parse(vsonSchemaStr));
    GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("member_id", 1);
    record.put("score", 2f);

    testReader(vsonSchemaStr, () -> record, (vsonObject) -> {
      Assert.assertEquals(((Map) vsonObject).get("member_id"), 1);
      Assert.assertEquals(((Map) vsonObject).get("score"), 2f);
    });

    record.put("score", null);
    testReader(vsonSchemaStr, () -> record, (vsonObject) -> {
      Assert.assertEquals(((Map) vsonObject).get("score"), null);
    });

    testReadNullValue(vsonSchemaStr);
  }

  @Test
  public void testReaderCanReadList() throws IOException {
    String vsonSchemaStr = "[\"int32\"]";
    List<Integer> record = Arrays.asList(1, 2, null);

    testReader(vsonSchemaStr, () -> record, (vsonObject) -> {
      Assert.assertTrue(vsonObject instanceof ArrayList, "VsonAvroDatumReader should return an ArrayList for 'Array' schema");

      Assert.assertEquals(((List) vsonObject).get(0), 1);
      Assert.assertEquals(((List) vsonObject).get(1), 2);
      Assert.assertEquals(((List) vsonObject).get(2), null);

      try {
        ((List) vsonObject).get(3);
        Assert.fail();
      } catch (IndexOutOfBoundsException e) {}
    });

    testReadNullValue(vsonSchemaStr);
  }


  @Test
  public void testListCompare() {
    List<Integer> list1 = Arrays.asList(1, 2, 3);
    List<Integer> list2 = Arrays.asList(1, 2, 3);
    Assert.assertEquals(list1, list2, "ArrayList should support equal function properly");
  }

  private void testReader(String vsonSchemaStr, Supplier valueSupplier) throws IOException {
    testReader(vsonSchemaStr, valueSupplier, (vsonObject) -> Assert.assertEquals(vsonObject, valueSupplier.get()));
    testReadNullValue(vsonSchemaStr);
  }

  private void testReadNullValue(String vsonSchemaStr) throws IOException {
    testReader(vsonSchemaStr, () -> null, (vsonObject) -> Assert.assertEquals(vsonObject, null));
  }

  private void testReader(String vsonSchemaStr, Supplier valueSupplier, Consumer valueValidator) throws IOException {
    Schema avroSchema = VsonAvroSchemaAdapter.parse(vsonSchemaStr);
    AvroGenericSerializer avroWriter = new AvroGenericSerializer(avroSchema);

    byte[] avroBytes = avroWriter.serialize(valueSupplier.get());
    BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(avroBytes, null);
    VsonAvroDatumReader reader = new VsonAvroDatumReader(avroSchema);

    Object vsonObject = reader.read(null, decoder);
    valueValidator.accept(vsonObject);
  }

  @Test
  public void testDeepEqualsHashMap() {
    Map<String, Object> deMap1 = new VsonAvroDatumReader.DeepEqualsHashMap();
    deMap1.put("key1", "value1");
    deMap1.put("key2", "value2".getBytes());

    Map<String, Object> deMap2 = new VsonAvroDatumReader.DeepEqualsHashMap();
    deMap2.put("key1", "value1");
    deMap2.put("key2", "value2".getBytes());

    Map<String, Object> regularMap1 = new HashMap<>();
    regularMap1.put("key1", "value1");
    regularMap1.put("key2", "value2".getBytes());

    Map<String, Object> regularMap2 = new HashMap<>();
    regularMap2.put("key1", "value1");
    regularMap2.put("key2", "value2".getBytes());

    String assertMessageForDeepEqualsHashMap = "DeepEqualsHashMap should implement deep equal properly even"
        + " it contains array elements.";
    String assertMessageForRegularHashMap = "HashMap supports deep equals() properly even it contains array elements so"
        + " the DeepEqualsHashMap is not necessary anymore.";

    Assert.assertTrue(deMap1.equals(deMap2), assertMessageForDeepEqualsHashMap);
    Assert.assertTrue(deMap1.equals(regularMap1), assertMessageForDeepEqualsHashMap);
    Assert.assertFalse(regularMap1.equals(deMap1), assertMessageForRegularHashMap);
    Assert.assertFalse(regularMap1.equals(regularMap2), assertMessageForRegularHashMap);
  }
}
