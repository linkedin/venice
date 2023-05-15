package com.linkedin.venice.serializer;

import static org.testng.Assert.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.commons.lang.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroSerializerTest {
  private static final String value = "abc";
  private static final Schema schema = AvroCompatibilityHelper.parse("\"string\"");
  private static final RecordSerializer<String> serializer = new AvroSerializer<>(schema);

  @Test
  public void testSerialize() {
    byte[] serializedValue = serializer.serialize(value);
    Assert.assertTrue(serializedValue.length > value.getBytes().length);
  }

  @Test
  public void testSerializeObjects() {
    List<String> array = Arrays.asList(value, value);
    byte[] serializedValue = serializer.serialize(value);
    byte[] expectedSerializedArray = ArrayUtils.addAll(serializedValue, serializedValue);
    Assert.assertEquals(serializer.serializeObjects(array), expectedSerializedArray);

    byte[] prefixBytes = "prefix".getBytes();
    Assert.assertEquals(
        serializer.serializeObjects(array, ByteBuffer.wrap(prefixBytes)),
        ArrayUtils.addAll(prefixBytes, expectedSerializedArray));
  }
}
