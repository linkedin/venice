package com.linkedin.venice.schema;

import static com.linkedin.venice.serializer.SerializerDeserializerFactory.getAvroGenericSerializer;
import static org.testng.Assert.assertFalse;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.AvroSpecificDeserializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


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
    RecordDeserializer<Object> deserializer =
        SerializerDeserializerFactory.getAvroGenericDeserializer(v1Schema, v3Schema);
    deserializer.deserialize(bytes);
  }

  @Test
  public void testStringMapInPartitionState() throws IOException {
    PartitionState ps = new PartitionState();
    ps.offset = 0;
    ps.offsetLag = 0;
    ps.endOfPush = false;
    ps.lastUpdate = 0;
    ps.databaseInfo = Collections.emptyMap();
    ps.leaderOffset = 0;
    // Populate a fake map
    Map<String, Long> upstreamOffsetMap = new HashMap<>();
    upstreamOffsetMap.put("fake_kafak_url", 100L);
    ps.upstreamOffsetMap = upstreamOffsetMap;
    ps.producerStates = Collections.emptyMap();
    ps.previousStatuses = Collections.emptyMap();
    ps.pendingReportIncrementalPushVersions = Collections.emptyList();
    ps.setRealtimeTopicProducerStates(Collections.emptyMap());

    AvroSerializer serializer = new AvroSerializer(ps.getSchema());
    byte[] serializedBytes = serializer.serialize(ps);
    /* Test the specific deserializer */
    AvroSpecificDeserializer<PartitionState> specificDeserializer =
        new AvroSpecificDeserializer<>(ps.getSchema(), PartitionState.class);
    PartitionState specificDeserializedObject = specificDeserializer.deserialize(serializedBytes);
    Assert.assertTrue(specificDeserializedObject.upstreamOffsetMap != null);
    for (Map.Entry<String, Long> entry: specificDeserializedObject.upstreamOffsetMap.entrySet()) {
      Assert.assertTrue(
          entry.getKey() instanceof String,
          "The key object type should be 'String', but got " + entry.getKey().getClass());
    }
    /* Test the generic deserializer */
    AvroGenericDeserializer<GenericRecord> genericDeserializer =
        new AvroGenericDeserializer<>(ps.getSchema(), ps.getSchema());
    GenericRecord genericDeserializedObject = genericDeserializer.deserialize(serializedBytes);
    Object upstreamOffsetMapObject = genericDeserializedObject.get("upstreamOffsetMap");
    Assert.assertTrue(upstreamOffsetMapObject instanceof Map);
    for (Map.Entry<Object, Object> entry: ((Map<Object, Object>) upstreamOffsetMapObject).entrySet()) {
      Assert.assertTrue(
          entry.getKey() instanceof String,
          "The key object type should be 'String', but got " + entry.getKey().getClass());
    }

    /* Try to de-serialize it with an older schema */
    Schema v10Schema = Utils.getSchemaFromResource("avro/PartitionState/v10/PartitionState.avsc");
    AvroGenericDeserializer<GenericRecord> genericDeserializerForV10 =
        new AvroGenericDeserializer<>(v10Schema, v10Schema);
    GenericRecord genericDeserializedObjectFromV10 = genericDeserializerForV10.deserialize(serializedBytes);
    Object upstreamOffsetMapObjectFromV10 = genericDeserializedObjectFromV10.get("upstreamOffsetMap");
    Assert.assertTrue(upstreamOffsetMapObjectFromV10 instanceof Map);
    for (Map.Entry<Object, Object> entry: ((Map<Object, Object>) upstreamOffsetMapObjectFromV10).entrySet()) {
      Assert.assertTrue(
          entry.getKey() instanceof Utf8,
          "The key object type should be 'Utf8', but got " + entry.getKey().getClass());
    }
  }

  @Test
  public void testFilterSchemaWithDefaultValueChanges() {
    Schema schema1 = Schema.parse(
        "{" + "\"fields\": ["
            + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
            + "   {\"default\": 0, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
            + "   ]," + " \"name\": \"testObject\", \"type\": \"record\"" + "}");
    Schema schema2 = Schema.parse(
        "{" + "\"fields\": ["
            + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
            + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
            + "   ]," + " \"name\": \"testObject\", \"type\": \"record\"" + "}");
    SchemaEntry schemaEntry1 = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, schema1);
    SchemaEntry schemaEntry2 = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, schema2);
    List<SchemaEntry> schemas = new ArrayList<>();
    schemas.add(schemaEntry1);
    schemas.add(schemaEntry2);
    List<SchemaEntry> matchedSchemas = AvroSchemaUtils.filterCanonicalizedSchemas(schemaEntry2, schemas);
    Assert.assertEquals(
        matchedSchemas.size(),
        schemas.size(),
        "The two canonicalized schemas should be considered indifferent");
    matchedSchemas = AvroSchemaUtils.filterSchemas(schemaEntry2, schemas);
    Assert.assertEquals(matchedSchemas.size(), 1, "There should only be one schema that matches");
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
    record.put("foo", AvroCompatibilityHelper.newEnumSymbol(schemaWithoutNamespace.getField("foo").schema(), "B"));
    String testString = "test";
    record.put("boo", testString);
    byte[] bytes = serializer.serialize(record);
    NamespaceTest readRecord;
    try {
      deserializer.deserialize(bytes);
      Assert.fail("Deserialization was suppose to fail");
    } catch (VeniceException e) {
      System.out.println(e.getCause());
      Assert.assertTrue(
          e.getCause().getMessage().contains("Found EnumType, expecting com.linkedin.venice.schema.EnumType"));
    }
    // Using toString() here because the schema is very simple in this test. Otherwise original schema string is
    // preferred.
    if (AvroSchemaUtils.schemaResolveHasErrors(schemaWithoutNamespace, schemaWithNamespace)) {
      Schema massagedWriterSchema = AvroSchemaUtils
          .generateSchemaWithNamespace(schemaWithoutNamespace.toString(), schemaWithNamespace.getNamespace());
      assertFalse(
          AvroSchemaUtils.schemaResolveHasErrors(massagedWriterSchema, schemaWithNamespace),
          "Resolve error should go away after the writer schema fix");
      deserializer =
          SerializerDeserializerFactory.getAvroSpecificDeserializer(massagedWriterSchema, NamespaceTest.class);
      readRecord = deserializer.deserialize(bytes);
      Assert.assertEquals(
          readRecord.getFoo(),
          EnumType.B,
          "Deserialized object field value should match with the value that was originally set");
      Assert.assertEquals(
          readRecord.getBoo().toString(),
          testString,
          "Deserialized object field value should match with the value that was originally set");
    } else {
      Assert.fail(
          "schemaResolveHasErrors should detect the missing namespace error between the reader and writer schema");
    }
  }
}
