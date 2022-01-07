package com.linkedin.venice.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.serializer.SerializerDeserializerFactory.*;


public class TestAvroSchema {
  @Test
  public void testCompatibilityForPartitionState() throws IOException {
    Schema v1Schema = Utils.getSchemaFromResource("avro/PartitionState/v1/PartitionState.avsc");
    Schema v3Schema = Utils.getSchemaFromResource("avro/PartitionState/v3/PartitionState.avsc");

    GenericData.Record v1Record = new GenericData.Record(v1Schema);
    v1Record.put("offset", Long.valueOf(123));
    v1Record.put("endOfPush", Boolean.TRUE);
    v1Record.put("lastUpdate", Long.valueOf(-1));
    v1Record.put("producerStates", new HashMap<>());

    RecordSerializer<Object> serializer = getAvroGenericSerializer(v1Schema);
    byte[] bytes = serializer.serialize(v1Record);

    // No exception should be thrown here
    RecordDeserializer<Object> deserializer = SerializerDeserializerFactory.getAvroGenericDeserializer(v1Schema, v3Schema);
    deserializer.deserialize(bytes);
  }

  @Test
  public void testFilterSchemaWithDefaultValueChanges() {
    Schema schema1 = Schema.parse("{"
        + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
        + "   {\"default\": 0, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
        + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\""
        +"}");
    Schema schema2 = Schema.parse("{"
        + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
        + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
        + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\""
        +"}");
    SchemaEntry schemaEntry1 = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, schema1);
    SchemaEntry schemaEntry2 = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, schema2);
    List<SchemaEntry> schemas  = new ArrayList<>();
    schemas.add(schemaEntry1);
    schemas.add(schemaEntry2);
    List<SchemaEntry> matchedSchemas = AvroSchemaUtils.filterCanonicalizedSchemas(schemaEntry2, schemas);
    Assert.assertEquals(matchedSchemas.size(), schemas.size(),
        "The two canonicalized schemas should be considered indifferent");
    matchedSchemas = AvroSchemaUtils.filterSchemas(schemaEntry2, schemas);
    Assert.assertEquals(matchedSchemas.size(), 1,
        "There should only be one schema that matches");
  }

  @Test
  public void testReaderWriterSchemaMissingNamespace() throws Exception {
    Schema schemaWithNamespace = NamespaceTest.SCHEMA$;
    Schema schemaWithoutNamespace = Utils.getSchemaFromResource("testSchemaWithoutNamespace.avsc");
    RecordSerializer<SpecificData.Record> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(schemaWithoutNamespace);
    RecordDeserializer<NamespaceTest> deserializer =
        SerializerDeserializerFactory.getAvroSpecificDeserializer(schemaWithoutNamespace, NamespaceTest.class);
    SpecificData.Record record = new SpecificData.Record(schemaWithoutNamespace);
    record.put("foo", AvroCompatibilityHelper.newEnumSymbol(
        schemaWithoutNamespace.getField("foo").schema(), "B"));
    String testString = "test";
    record.put("boo", testString);
    byte[] bytes = serializer.serialize(record);
    NamespaceTest readRecord;
    try {
      readRecord = deserializer.deserialize(bytes);
      Assert.fail("Deserialization was suppose to fail");
    } catch (VeniceException e) {
      System.out.println(e.getCause());
      Assert.assertTrue(
          e.getCause().getMessage().contains("Found EnumType, expecting com.linkedin.venice.schema.EnumType"));
    }
    // Using toString() here because the schema is very simple in this test. Otherwise original schema string is preferred.
    if (AvroSchemaUtils.schemaResolveHasErrors(schemaWithoutNamespace, schemaWithNamespace)) {
      Schema massagedWriterSchema =
          AvroSchemaUtils.generateSchemaWithNamespace(schemaWithoutNamespace.toString(), schemaWithNamespace.getNamespace());
      Assert.assertFalse(AvroSchemaUtils.schemaResolveHasErrors(massagedWriterSchema, schemaWithNamespace),
          "Resolve error should go away after the writer schema fix");
      deserializer =
          SerializerDeserializerFactory.getAvroSpecificDeserializer(massagedWriterSchema, NamespaceTest.class);
      readRecord = deserializer.deserialize(bytes);
      Assert.assertEquals(readRecord.getFoo(), EnumType.B,
          "Deserialized object field value should match with the value that was originally set");
      Assert.assertEquals(readRecord.getBoo().toString(), testString,
          "Deserialized object field value should match with the value that was originally set");
    } else {
      Assert.fail("schemaResolveHasErrors should detect the missing namespace error between the reader and writer schema");
    }
  }
}
