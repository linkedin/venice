package com.linkedin.venice.schema.vson;

import static com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter.stripFromUnion;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.serializer.AvroSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


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
    ThreadLocalRandom.current().nextBytes(randomBytes);
    testReader(
        byteSchema,
        () -> ByteBuffer.wrap(randomBytes),
        (vsonObject) -> Assert.assertEquals(vsonObject, randomBytes));
    testReadNullValue(byteSchema);

    // single byte
    byte[] singleByteArray = { randomBytes[0] };
    GenericData.Fixed fixedByte = AvroCompatibilityHelper.newFixedField(
        Schema.createFixed(
            VsonAvroSchemaAdapter.BYTE_WRAPPER,
            VsonAvroSchemaAdapter.DEFAULT_DOC,
            VsonAvroSchemaAdapter.DEFAULT_NAMESPACE,
            1),
        singleByteArray);
    testReader("\"int8\"", () -> fixedByte, vsonObject -> Assert.assertEquals(vsonObject, randomBytes[0]));
    testReadNullValue("\"int8\"");

    // short
    byte[] shortBytesArray = { (byte) 0xFF, (byte) 0xFE }; // 0xFFFE = -2
    GenericData.Fixed fixedShort = AvroCompatibilityHelper.newFixedField(
        Schema.createFixed(
            VsonAvroSchemaAdapter.SHORT_WRAPPER,
            VsonAvroSchemaAdapter.DEFAULT_DOC,
            VsonAvroSchemaAdapter.DEFAULT_NAMESPACE,
            2),
        shortBytesArray);
    testReader("\"int16\"", () -> fixedShort, vsonObject -> Assert.assertEquals(vsonObject, (short) -2));
    testReadNullValue("\"int16\"");
  }

  @Test
  public void testReaderCanReadRecord() throws IOException {
    String vsonSchemaStr = "{\"member_id\":\"int32\", \"score\":\"float32\"}";

    // strip the top level union as this is intended for constructing Avro GenericData.Record
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
      Assert.assertNull(((Map) vsonObject).get("score"));
    });

    testReadNullValue(vsonSchemaStr);
  }

  @Test
  public void testReaderCanReadList() throws IOException {
    String vsonSchemaStr = "[\"int32\"]";
    List<Integer> record = Arrays.asList(1, 2, null);

    testReader(vsonSchemaStr, () -> record, (vsonObject) -> {
      Assert.assertTrue(
          vsonObject instanceof VsonAvroDatumReader.DeepEqualsArrayList,
          "VsonAvroDatumReader should return a DeepEqualsArrayList for 'Array' schema");

      Assert.assertEquals(((List) vsonObject).get(0), 1);
      Assert.assertEquals(((List) vsonObject).get(1), 2);
      Assert.assertNull(((List) vsonObject).get(2));

      try {
        ((List) vsonObject).get(3);
        Assert.fail();
      } catch (IndexOutOfBoundsException e) {
      }
    });

    testReadNullValue(vsonSchemaStr);
  }

  private void testReader(String vsonSchemaStr, Supplier valueSupplier) throws IOException {
    testReader(vsonSchemaStr, valueSupplier, (vsonObject) -> Assert.assertEquals(vsonObject, valueSupplier.get()));
    testReadNullValue(vsonSchemaStr);
  }

  private void testReadNullValue(String vsonSchemaStr) throws IOException {
    testReader(vsonSchemaStr, () -> null, Assert::assertNull);
  }

  private void testReader(String vsonSchemaStr, Supplier valueSupplier, Consumer valueValidator) throws IOException {
    Schema avroSchema = VsonAvroSchemaAdapter.parse(vsonSchemaStr);
    AvroSerializer avroWriter = new AvroSerializer(avroSchema);

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

    String assertMessageForDeepEqualsHashMap =
        "DeepEqualsHashMap should implement deep equal properly even" + " it contains byte[] value.";
    String assertMessageForRegularHashMap = "HashMap supports deep equals() properly even it contains byte[] value so"
        + " the DeepEqualsHashMap is not necessary anymore.";

    Assert.assertTrue(deMap1.equals(deMap2), assertMessageForDeepEqualsHashMap);
    Assert.assertTrue(deMap1.equals(regularMap1), assertMessageForDeepEqualsHashMap);
    Assert.assertFalse(regularMap1.equals(deMap1), assertMessageForRegularHashMap);
    Assert.assertFalse(regularMap1.equals(regularMap2), assertMessageForRegularHashMap);
  }

  @Test
  public void testDeepEqualsArrayList() {
    List<Object> deList1 = new VsonAvroDatumReader.DeepEqualsArrayList();
    deList1.add("value1".getBytes());
    deList1.add("value2".getBytes());

    List<Object> deList2 = new VsonAvroDatumReader.DeepEqualsArrayList();
    deList2.add("value1".getBytes());
    deList2.add("value2".getBytes());

    List<Object> regularList1 = new ArrayList<>();
    regularList1.add("value1".getBytes());
    regularList1.add("value2".getBytes());

    List<Object> regularList2 = new ArrayList<>();
    regularList2.add("value1".getBytes());
    regularList2.add("value2".getBytes());

    String assertMessageForDeepEqualsArrayList =
        "DeepEqualsArrayList should implement deep equal properly even" + " it contains byte[] elements.";
    String assertMessageForRegularArrayList =
        "ArrayList supports deep equals() properly even it contains byte[] elements so"
            + " the DeepEqualsArrayList is not necessary anymore.";

    Assert.assertTrue(deList1.equals(deList2), assertMessageForDeepEqualsArrayList);
    Assert.assertTrue(deList1.equals(regularList1), assertMessageForDeepEqualsArrayList);
    Assert.assertFalse(regularList1.equals(deList1), assertMessageForRegularArrayList);
    Assert.assertFalse(regularList1.equals(regularList2), assertMessageForRegularArrayList);

    // Test map with array element
    Map<String, Object> deMap = new VsonAvroDatumReader.DeepEqualsHashMap();
    Map<String, Object> regularMap = new HashMap<>();
    deMap.put("key1", deList1);
    deMap.put("key2", "value".getBytes());
    regularMap.put("key1", regularList1);
    regularMap.put("key2", "value".getBytes());

    Assert.assertTrue(deMap.equals(regularMap), "DeepEqualsHashMap should support proper equal recursively");

    // Test array with map element
    List<Object> deList = new VsonAvroDatumReader.DeepEqualsArrayList();
    List<Object> regularList = new ArrayList<>();
    deList.add(deMap);
    regularList.add(regularMap);
    Assert.assertTrue(deList.equals(regularList), "DeepEqualsArrayList should support proper equal recursively");
  }
}
