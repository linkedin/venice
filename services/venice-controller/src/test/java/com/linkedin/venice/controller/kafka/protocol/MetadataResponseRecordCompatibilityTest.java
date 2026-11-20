package com.linkedin.venice.controller.kafka.protocol;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetadataResponseRecordCompatibilityTest extends ProtocolCompatibilityTest {
  @Test
  public void testV4V5WireCompatibility() throws IOException, InterruptedException {
    Schema v4 = Utils.getSchemaFromResource("avro/MetadataResponseRecord/v4/MetadataResponseRecord.avsc");
    Schema v5 = Utils.getSchemaFromResource("avro/MetadataResponseRecord/v5/MetadataResponseRecord.avsc");
    Map<Integer, Schema> schemaMap = new HashMap<>();
    schemaMap.put(4, v4);
    schemaMap.put(5, v5);
    testProtocolCompatibility(schemaMap, 5);

    GenericRecord v4Record = createMetadataRecord(v4);
    byte[] v4Bytes = SerializerDeserializerFactory.getAvroGenericSerializer(v4).serialize(v4Record);
    GenericRecord v5Reader =
        SerializerDeserializerFactory.<GenericRecord>getAvroGenericDeserializer(v4, v5).deserialize(v4Bytes);
    Assert.assertEquals(v5Reader.get("multiKeyLongTailRetryThresholdsInMs").toString(), "");
    MetadataResponseRecord specificReader =
        FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(v4, MetadataResponseRecord.class)
            .deserialize(v4Bytes);
    Assert.assertEquals(specificReader.getMultiKeyLongTailRetryThresholdsInMs().toString(), "");

    GenericRecord v5Record = createMetadataRecord(v5);
    v5Record.put("batchGetLimit", 500);
    v5Record.put("multiKeyLongTailRetryThresholdsInMs", "1-:8");
    byte[] v5Bytes = SerializerDeserializerFactory.getAvroGenericSerializer(v5).serialize(v5Record);
    GenericRecord v4Reader =
        SerializerDeserializerFactory.<GenericRecord>getAvroGenericDeserializer(v5, v4).deserialize(v5Bytes);
    Assert.assertEquals(v4Reader.get("batchGetLimit"), 500);
    Assert.assertNull(v4Reader.getSchema().getField("multiKeyLongTailRetryThresholdsInMs"));
  }

  @Test
  public void testV6FutureVersionTypeWireCompatibility() throws IOException {
    Schema v5 = Utils.getSchemaFromResource("avro/MetadataResponseRecord/v5/MetadataResponseRecord.avsc");
    Schema v6 = Utils.getSchemaFromResource("avro/MetadataResponseRecord/v6/MetadataResponseRecord.avsc");
    GenericRecord v6Record = createMetadataRecord(v6);
    v6Record.put("currentVersionType", 3);
    v6Record.put("batchGetLimit", 500);
    v6Record.put("multiKeyLongTailRetryThresholdsInMs", "1-:8");

    byte[] v6Bytes = SerializerDeserializerFactory.getAvroGenericSerializer(v6).serialize(v6Record);
    GenericRecord v5Reader =
        SerializerDeserializerFactory.<GenericRecord>getAvroGenericDeserializer(v6, v5).deserialize(v6Bytes);
    Assert.assertEquals(v5Reader.get("batchGetLimit"), 500);
    Assert.assertEquals(v5Reader.get("multiKeyLongTailRetryThresholdsInMs").toString(), "1-:8");
    Assert.assertNull(v5Reader.getSchema().getField("currentVersionType"));
    MetadataResponseRecord v6Reader =
        FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(v6, MetadataResponseRecord.class)
            .deserialize(v6Bytes);
    Assert.assertEquals(v6Reader.getCurrentVersionType(), 3);

    byte[] v5Bytes = SerializerDeserializerFactory.getAvroGenericSerializer(v5).serialize(createMetadataRecord(v5));
    MetadataResponseRecord upgradedReader =
        FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(v5, MetadataResponseRecord.class)
            .deserialize(v5Bytes);
    Assert.assertEquals(upgradedReader.getCurrentVersionType(), 0);
  }

  private GenericRecord createMetadataRecord(Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    for (Schema.Field field: schema.getFields()) {
      record.put(
          field.name(),
          "versions".equals(field.name())
              ? Collections.emptyList()
              : AvroCompatibilityHelper.getGenericDefaultValue(field));
    }
    return record;
  }

  @Test
  public void testMetadataResponseRecordCompatibility() throws InterruptedException {
    Map<Integer, Schema> schemaMap = initMetadataResponseRecordSchemaMap();
    Assert.assertFalse(schemaMap.isEmpty());
    Assert.assertEquals(
        MetadataResponseRecord.SCHEMA$,
        schemaMap.get(AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion()));
    testProtocolCompatibility(schemaMap, schemaMap.size());
  }

  private Map<Integer, Schema> initMetadataResponseRecordSchemaMap() {
    Map<Integer, Schema> metadataResponseRecordSchemaMap = new HashMap<>();
    try {
      for (int i = 1; i <= AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion(); i++) {
        metadataResponseRecordSchemaMap
            .put(i, Utils.getSchemaFromResource("avro/MetadataResponseRecord/v" + i + "/MetadataResponseRecord.avsc"));
      }
      return metadataResponseRecordSchemaMap;
    } catch (IOException e) {
      throw new VeniceMessageException("Failed to load schema from resource");
    }
  }
}
