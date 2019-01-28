package com.linkedin.venice.schema;

import com.linkedin.venice.common.AvroSchemaUtils;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAvroSchema {
  @Test
  public void testCompatibilityForPartitionState() throws IOException {
    Schema v1Schema = Utils.getSchemaFromResource("avro/PartitionState/v1/PartitionState.avsc");
    Schema v3Schema = Utils.getSchemaFromResource("avro/PartitionState/v3/PartitionState.avsc");

    GenericData.Record v1Record = new GenericData.Record(v1Schema);
    v1Record.put("offset", new Long(123));
    v1Record.put("endOfPush", new Boolean(true));
    v1Record.put("lastUpdate", new Long(-1));
    v1Record.put("producerStates", new HashMap<>());

    RecordSerializer<Object> serializer = SerializerDeserializerFactory.getAvroGenericSerializer(v1Schema);
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
}
