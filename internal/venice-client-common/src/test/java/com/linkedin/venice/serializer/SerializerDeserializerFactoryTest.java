package com.linkedin.venice.serializer;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.schemas.TestValueRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecordWithMoreFields;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SerializerDeserializerFactoryTest {
  @Test
  public void getAvroGenericSerializerTest() throws VeniceClientException {
    String schemaStr = "\"string\"";
    Schema schema = Schema.parse(schemaStr);
    String stringValue = "abc";
    RecordSerializer<Object> serializer = SerializerDeserializerFactory.getAvroGenericSerializer(schema);
    Assert.assertNotEquals(serializer.serialize(stringValue), stringValue.getBytes());

    RecordSerializer<Object> anotherSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(schema);
    Assert.assertTrue(anotherSerializer == serializer);
  }

  @Test
  public void getAvroGenericDeserializerTest() throws VeniceClientException {
    Schema actualSchema = TestValueRecord.SCHEMA$;
    Schema expectedSchema = TestValueRecordWithMoreFields.SCHEMA$;

    GenericData.Record actualObject = new GenericData.Record(actualSchema);
    actualObject.put("long_field", 1000l);
    actualObject.put("string_field", "abc");
    RecordSerializer<Object> serializer = SerializerDeserializerFactory.getAvroGenericSerializer(actualSchema);
    byte[] serializedValue = serializer.serialize(actualObject);

    RecordDeserializer<GenericData.Record> deserializer =
        SerializerDeserializerFactory.getAvroGenericDeserializer(actualSchema, expectedSchema);

    GenericData.Record expectedRecord = deserializer.deserialize(serializedValue);
    Assert.assertNotNull(expectedRecord);
    Assert.assertEquals(expectedRecord.get("long_field"), 1000l);
    Assert.assertEquals(expectedRecord.get("string_field").toString(), "abc");
    Assert.assertEquals(expectedRecord.get("int_field"), 10);

    RecordDeserializer<GenericData.Record> anotherDeserializer =
        SerializerDeserializerFactory.getAvroGenericDeserializer(actualSchema, expectedSchema);
    Assert.assertTrue(anotherDeserializer == deserializer);
  }

  @Test
  public void getAvroSpecificDeserializerTest() throws IOException, VeniceClientException {
    Schema actualSchema = TestValueRecord.SCHEMA$;

    GenericData.Record actualObject = new GenericData.Record(actualSchema);
    actualObject.put("long_field", 1000l);
    actualObject.put("string_field", "abc");

    RecordSerializer<Object> serializer = SerializerDeserializerFactory.getAvroGenericSerializer(actualSchema);
    byte[] serializedValue = serializer.serialize(actualObject);

    RecordDeserializer<TestValueRecordWithMoreFields> deserializer =
        SerializerDeserializerFactory.getAvroSpecificDeserializer(actualSchema, TestValueRecordWithMoreFields.class);

    TestValueRecordWithMoreFields expectedObject = deserializer.deserialize(serializedValue);
    Assert.assertEquals(expectedObject.long_field, 1000l);
    Assert.assertEquals(expectedObject.string_field.toString(), "abc");
    Assert.assertEquals(expectedObject.int_field, 10);

    RecordDeserializer<TestValueRecordWithMoreFields> anotherDeserializer =
        SerializerDeserializerFactory.getAvroSpecificDeserializer(actualSchema, TestValueRecordWithMoreFields.class);
    Assert.assertTrue(anotherDeserializer == deserializer);
  }
}
