package com.linkedin.venice.serializer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroSerializerTest {
  private static final String value = "abc";
  private static final Schema schema = AvroCompatibilityHelper.parse("\"string\"");
  private static final RecordSerializer<String> RECORD_SERIALIZER = new AvroSerializer<>(schema);
  private static final String MAP_FIELD_1 = "mapField1";
  private static final String MAP_FIELD_2 = "mapField2";
  private static final String STRING_FIELD_1 = "stringField1";
  private static final String UNION_FIELD_1 = "unionField1";
  private static final Schema recordSchema = AvroCompatibilityHelper.parse(
      "{" //
          + "    \"type\": \"record\"," //
          + "    \"namespace\": \"com.linkedin.avro\"," //
          + "    \"name\": \"Person\"," //
          + "    \"fields\": [" //
          + "        {" //
          + "            \"name\": \"" + MAP_FIELD_1 + "\"," //
          + "            \"type\": {" //
          + "                \"type\": \"map\"," //
          + "                \"values\": \"string\"" //
          + "            }" //
          + "        }" //
          + "    ]" //
          + "}");

  private static final Schema recordSchemaWithOneMoreFieldAtTheEnd = AvroCompatibilityHelper.parse(
      "{" //
          + "    \"type\": \"record\"," //
          + "    \"namespace\": \"com.linkedin.avro\"," //
          + "    \"name\": \"Person\"," //
          + "    \"fields\": [" //
          + "        {" //
          + "            \"name\": \"" + MAP_FIELD_1 + "\"," //
          + "            \"type\": {" //
          + "                \"type\": \"map\"," //
          + "                \"values\": \"string\"" //
          + "            }" //
          + "        }, {" //
          + "            \"name\": \"" + MAP_FIELD_2 + "\"," //
          + "            \"type\": {" //
          + "                \"type\": \"map\"," //
          + "                \"values\": \"string\"" //
          + "            }" //
          + "        }" //
          + "    ]" //
          + "}");

  private static final Schema recordSchemaWithOnlyMapField2 = AvroCompatibilityHelper.parse(
      "{" //
          + "    \"type\": \"record\"," //
          + "    \"namespace\": \"com.linkedin.avro\"," //
          + "    \"name\": \"Person\"," //
          + "    \"fields\": [" //
          + "        {" //
          + "            \"name\": \"" + MAP_FIELD_2 + "\"," //
          + "            \"type\": {" //
          + "                \"type\": \"map\"," //
          + "                \"values\": \"string\"" //
          + "            }" //
          + "        }" //
          + "    ]" //
          + "}");

  private static final Schema recordSchemaWithAnotherField = AvroCompatibilityHelper.parse(
      "{" //
          + "    \"type\": \"record\"," //
          + "    \"namespace\": \"com.linkedin.avro\"," //
          + "    \"name\": \"Person\"," //
          + "    \"fields\": [" //
          + "        {" //
          + "            \"name\": \"" + STRING_FIELD_1 + "\"," //
          + "            \"type\": \"string\"" //
          + "        }" //
          + "    ]" //
          + "}");

  private static final Schema recordSchemaWithAnotherFieldOfTheSameNameButDifferentType = AvroCompatibilityHelper.parse(
      "{" //
          + "    \"type\": \"record\"," //
          + "    \"namespace\": \"com.linkedin.avro\"," //
          + "    \"name\": \"Person\"," //
          + "    \"fields\": [" //
          + "        {" //
          + "            \"name\": \"" + MAP_FIELD_1 + "\"," //
          + "            \"type\": \"string\"" //
          + "        }" //
          + "    ]" //
          + "}");

  private static final Schema recordSchemaWithWeirdUnionField = AvroCompatibilityHelper.parse(
      "{" //
          + "    \"type\": \"record\"," //
          + "    \"namespace\": \"com.linkedin.avro\"," //
          + "    \"name\": \"Person\"," //
          + "    \"fields\": [" //
          + "        {" //
          + "            \"name\": \"" + UNION_FIELD_1 + "\"," //
          + "            \"type\": [\"boolean\", \"string\"]" //
          + "        }" //
          + "    ]" //
          + "}");

  @Test
  public void testSerialize() {
    byte[] serializedValue = RECORD_SERIALIZER.serialize(value);
    Assert.assertTrue(serializedValue.length > value.getBytes().length);
  }

  @Test
  public void testSerializeObjects() {
    List<String> array = Arrays.asList(value, value);
    byte[] serializedValue = RECORD_SERIALIZER.serialize(value);
    byte[] expectedSerializedArray = ArrayUtils.addAll(serializedValue, serializedValue);
    Assert.assertEquals(RECORD_SERIALIZER.serializeObjects(array), expectedSerializedArray);

    byte[] prefixBytes = "prefix".getBytes();
    Assert.assertEquals(
        RECORD_SERIALIZER.serializeObjects(array, ByteBuffer.wrap(prefixBytes)),
        ArrayUtils.addAll(prefixBytes, expectedSerializedArray));
  }

  @Test
  public void testDeterministicallySerializeMapWithDifferentSubclass() {
    AvroSerializer<GenericRecord> serializer = new AvroSerializer<>(recordSchema);
    GenericRecord record = getGenericRecordWithPopulatedMap();

    // Verify no exception is thrown
    serializer.serialize(record);
  }

  /**
   * In this test, we exercise many Avro edge cases, which could be throwing slightly different exceptions.
   */
  @Test
  public void testBadSchema() {
    // Serializer instantiated with a given schema
    AvroSerializer<GenericRecord> serializer = new AvroSerializer<>(recordSchema);

    // Record which corresponds exactly to the serializer's schema
    GenericRecord record1 = new GenericData.Record(recordSchema);
    Map<String, String> map1 = new HashMap<>();
    map1.put("k1", "v1");
    map1.put("k2", "v2");
    record1.put(MAP_FIELD_1, map1);

    // Record which contains a superset of the fields of the serializer's schema
    GenericRecord record2 = new GenericData.Record(recordSchemaWithOneMoreFieldAtTheEnd);
    Map<String, String> map2 = new HashMap<>();
    map2.put("k1", "v1");
    map2.put("k2", "v2");
    record2.put(MAP_FIELD_1, map2);
    Map<String, String> map3 = new HashMap<>();
    map3.put("k3", "v3");
    map3.put("k4", "v4");
    record2.put(MAP_FIELD_2, map3);

    byte[] bytes1 = serializer.serialize(record1);
    byte[] bytes2 = serializer.serialize(record2);
    assertEquals(
        bytes1,
        bytes2,
        "Encoding the expected schema first, then the superset schema, should yield equal payloads.");

    byte[] bytes3 = serializer.serialize(record2);
    byte[] bytes4 = serializer.serialize(record1);
    assertEquals(
        bytes3,
        bytes4,
        "Encoding the superset schema first, then the expected schema, should yield equal payloads.");

    // Record which contains a field with the same type but a different name than the serializer's schema
    GenericRecord record3 = new GenericData.Record(recordSchemaWithOnlyMapField2);
    Map<String, String> map4 = new HashMap<>();
    map4.put("k1", "v1");
    map4.put("k2", "v2");
    record2.put(MAP_FIELD_2, map4);

    // Encoding the schema with wrong fields, should fail...
    assertThrows(VeniceSerializationException.class, () -> serializer.serialize(record3));
    assertThatSerializerIsNotCorrupt(serializer, record1, bytes1);

    // Record which contains a field with another name and type than the serializer's schema
    GenericRecord record4 = new GenericData.Record(recordSchemaWithAnotherField);
    record4.put(STRING_FIELD_1, "Without stones, there is no arch.");

    // Encoding the schema with wrong fields, should fail...
    assertThrows(VeniceSerializationException.class, () -> serializer.serialize(record4));
    assertThatSerializerIsNotCorrupt(serializer, record1, bytes1);

    // Record which contains a field with the same name but another type than the serializer's schema
    GenericRecord record5 = new GenericData.Record(recordSchemaWithAnotherFieldOfTheSameNameButDifferentType);
    record5.put(MAP_FIELD_1, "Without stones, there is no arch.");

    // Encoding the schema with wrong fields, should fail...
    assertThrows(VeniceSerializationException.class, () -> serializer.serialize(record5));
    assertThatSerializerIsNotCorrupt(serializer, record1, bytes1);

    // Valid schema, but we fail to set a mandatory field
    GenericRecord record6 = new GenericData.Record(recordSchema);
    assertThrows(VeniceSerializationException.class, () -> serializer.serialize(record6));
    assertThatSerializerIsNotCorrupt(serializer, record1, bytes1);

    // Valid schema, but we set the wrong type
    GenericRecord record7 = new GenericData.Record(recordSchema);
    record7.put(MAP_FIELD_1, 1);
    assertThrows(VeniceSerializationException.class, () -> serializer.serialize(record7));
    assertThatSerializerIsNotCorrupt(serializer, record1, bytes1);

    // Serializer for a schema which contains a union field of ["boolean", "string"]
    AvroSerializer<GenericRecord> serializer2 = new AvroSerializer<>(recordSchemaWithWeirdUnionField);

    // Put an int into the ["boolean", "string"] union field
    GenericRecord record8 = new GenericData.Record(recordSchemaWithWeirdUnionField);
    record8.put(UNION_FIELD_1, 1);
    assertThrows(VeniceSerializationException.class, () -> serializer2.serialize(record8));
    assertThatSerializerIsNotCorrupt(serializer, record1, bytes1);

    // Put a record into the ["boolean", "string"] union field
    GenericRecord record9 = new GenericData.Record(recordSchemaWithWeirdUnionField);
    record9.put(UNION_FIELD_1, new GenericData.Record(recordSchema));
    assertThrows(VeniceSerializationException.class, () -> serializer2.serialize(record9));
    assertThatSerializerIsNotCorrupt(serializer, record1, bytes1);

    // Put null into the ["boolean", "string"] union field
    GenericRecord record10 = new GenericData.Record(recordSchemaWithWeirdUnionField);
    record10.put(UNION_FIELD_1, null);
    assertThrows(VeniceSerializationException.class, () -> serializer2.serialize(record10));
    assertThatSerializerIsNotCorrupt(serializer, record1, bytes1);
  }

  private void assertThatSerializerIsNotCorrupt(
      AvroSerializer<GenericRecord> serializer,
      GenericRecord validRecord,
      byte[] expectedBytes) {
    byte[] actualBytes = serializer.serialize(validRecord);
    assertEquals(
        expectedBytes,
        actualBytes,
        "Reusing the serializer with valid input should still work following a failure.");
  }

  /**
   * In this test, we use mocking to simulate any exception, not necessarily the specific exceptions thrown by Avro.
   */
  @Test
  public void testRecoveryFromCorruptInternalStateOfReusableEncoder() {
    AvroSerializerWithWriteFailure serializer = new AvroSerializerWithWriteFailure(recordSchema);
    GenericRecord record = getGenericRecordWithPopulatedMap();

    // First invocation is expected to throw.
    assertThrows(VeniceSerializationException.class, () -> serializer.serialize(record));

    // We should be able to serialize twice and get the same result everytime,
    // which would not be the case if the internal state is corrupted by the first exception
    byte[] bytes = serializer.serialize(record);
    Assert.assertEquals(serializer.serialize(record), bytes);
  }

  private static class AvroSerializerWithWriteFailure<T> extends AvroSerializer<T> {
    private boolean firstUse = true;

    public AvroSerializerWithWriteFailure(Schema schema) {
      super(schema);
    }

    @Override
    public void write(T object, Encoder encoder) throws IOException {
      super.write(object, encoder);
      if (firstUse) {
        firstUse = false;
        throw new IOException("Exception on first use only... subsequent invocations will succeed.");
      }
    }
  }

  private GenericRecord getGenericRecordWithPopulatedMap() {
    Map<Object, Object> map = new LinkedHashMap<>();
    map.put("key1", "valueStr");
    map.put(new Utf8("key2"), "valueUtf8");
    map.put(new Utf8("key3"), "valueUtf8_2");
    map.put("key4", "valueStr_2");
    map.put(10L, "valueStr_2");

    GenericRecord record = new GenericData.Record(recordSchema);
    record.put(MAP_FIELD_1, map);

    return record;
  }
}
