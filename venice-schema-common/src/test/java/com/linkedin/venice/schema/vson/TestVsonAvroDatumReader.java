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
      Assert.assertEquals(((List) vsonObject).get(0), 1);
      Assert.assertEquals(((List) vsonObject).get(1), 2);
      Assert.assertEquals(((List) vsonObject).get(2), null);

      try {
        ((List) vsonObject).get(3);
        Assert.fail();
      } catch (ArrayIndexOutOfBoundsException e) {}
    });

    testReadNullValue(vsonSchemaStr);
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
}
