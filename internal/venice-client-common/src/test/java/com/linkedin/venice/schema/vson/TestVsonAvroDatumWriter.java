package com.linkedin.venice.schema.vson;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVsonAvroDatumWriter {
  @Test
  public void testWriterCanWritePrimitive() throws IOException {
    testWriter("\"int32\"", () -> 123);
    testWriter("\"int64\"", () -> 123l);
    testWriter("\"int64\"", () -> 123, () -> 123l); // let integers be supplied for longs
    testWriter("\"float32\"", () -> 123f);
    testWriter("\"float64\"", () -> 123d);
    testWriter("\"boolean\"", () -> true);
    testWriter("\"string\"", () -> new Utf8("123"));

    byte[] randomBytes = new byte[10];
    ThreadLocalRandom.current().nextBytes(randomBytes);
    testWriter("\"bytes\"", () -> ByteBuffer.wrap(randomBytes));

    testWriter("\"int8\"", () -> randomBytes[0], avroObject -> {
      Assert.assertTrue(avroObject instanceof GenericData.Fixed);
      byte[] byteArray = ((GenericData.Fixed) avroObject).bytes();
      Assert.assertEquals(byteArray.length, 1);
      Assert.assertEquals(byteArray[0], randomBytes[0]);
    });
    testWriteNullValue("\"int8\"");

    testWriter("\"int16\"", () -> (short) -2, avroObject -> {
      Assert.assertTrue(avroObject instanceof GenericData.Fixed);
      byte[] byteArray = ((GenericData.Fixed) avroObject).bytes();
      Assert.assertEquals(byteArray.length, 2);
      // -2 is equal to 11111110 (0xFFFE)
      Assert.assertEquals(byteArray[0], (byte) 0xFF);
      Assert.assertEquals(byteArray[1], (byte) 0xFE);
    });
    testWriteNullValue("\"int16\"");
  }

  @Test
  public void testWriterCanWriteRecord() throws IOException {
    String vsonSchemaStr = "{\"member_id\":\"int32\", \"score\":\"float32\"}";
    HashMap<String, Object> record = new HashMap<>();
    record.put("member_id", 1);
    record.put("score", 2f);

    testWriter(vsonSchemaStr, () -> record, (avroObject) -> {
      Assert.assertEquals(((GenericData.Record) avroObject).get("member_id"), 1);
      Assert.assertEquals(((GenericData.Record) avroObject).get("score"), 2f);

      // test querying an invalid field. By default, Avro is gonna return null.
      Assert.assertNull(((GenericData.Record) avroObject).get("unknown field"));
    });

    // record with null field
    record.put("score", null);
    testWriter(vsonSchemaStr, () -> record, (avroObject) -> {
      Assert.assertNull(((GenericData.Record) avroObject).get("score"));
    });

    testWriteNullValue(vsonSchemaStr);
  }

  @Test
  public void testWriterCanWriteList() throws IOException {
    String vsonSchemaStr = "[\"int32\"]";
    List<Integer> record = Arrays.asList(1, 2, null);

    testWriter(vsonSchemaStr, () -> record, (avroObject) -> {
      Assert.assertEquals(((GenericData.Array) avroObject).get(0), 1);
      Assert.assertEquals(((GenericData.Array) avroObject).get(1), 2);
      Assert.assertNull(((GenericData.Array) avroObject).get(2));

      // test querying an invalid element
      try {
        ((GenericData.Array) avroObject).get(3);
        Assert.fail();
      } catch (IndexOutOfBoundsException e) {
      }
    });

    testWriteNullValue(vsonSchemaStr);
  }

  private void testWriter(String vsonSchemaStr, Supplier valueSupplier) throws IOException {
    testWriter(vsonSchemaStr, valueSupplier, valueSupplier);
  }

  private void testWriter(String vsonSchemaStr, Supplier valueSupplier, Supplier finalValueSupplier)
      throws IOException {
    testWriter(vsonSchemaStr, valueSupplier, (avroObject) -> Assert.assertEquals(avroObject, finalValueSupplier.get()));
    testWriteNullValue(vsonSchemaStr);
  }

  private void testWriteNullValue(String vsonSchemaStr) throws IOException {
    testWriter(vsonSchemaStr, () -> null, Assert::assertNull);
  }

  private void testWriter(String vsonSchemaStr, Supplier valueSupplier, Consumer valueValidator) throws IOException {
    Schema avroSchema = VsonAvroSchemaAdapter.parse(vsonSchemaStr);
    VsonAvroDatumWriter writer = new VsonAvroDatumWriter(avroSchema);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(output, true, null);
    writer.write(valueSupplier.get(), encoder);
    encoder.flush();

    AvroGenericDeserializer deserializer = new AvroGenericDeserializer(avroSchema, avroSchema);
    valueValidator.accept(deserializer.deserialize(output.toByteArray()));
  }
}
