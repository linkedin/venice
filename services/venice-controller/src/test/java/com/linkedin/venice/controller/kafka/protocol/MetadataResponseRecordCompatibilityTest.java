package com.linkedin.venice.controller.kafka.protocol;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetadataResponseRecordCompatibilityTest extends ProtocolCompatibilityTest {
  @Test
  public void testV4V5WireCompatibility() throws IOException {
    Schema v4 = Utils.getSchemaFromResource("avro/MetadataResponseRecord/v4/MetadataResponseRecord.avsc");
    Schema v5 = Utils.getSchemaFromResource("avro/MetadataResponseRecord/v5/MetadataResponseRecord.avsc");
    Assert.assertEquals(MetadataResponseRecord.SCHEMA$, v5);
    Assert.assertEquals(AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion(), 5);
    GenericRecord oldRecord = new GenericRecordBuilder(v4).set("versions", Collections.emptyList()).build();
    Assert.assertEquals(roundTrip(oldRecord, v5).get("multiKeyLongTailRetryThresholdsInMs").toString(), "");
    GenericRecord newRecord = new GenericRecordBuilder(v5).set("versions", Collections.emptyList())
        .set("multiKeyLongTailRetryThresholdsInMs", "1-:8")
        .set("batchGetLimit", 500)
        .build();
    GenericRecord oldReaderRecord = roundTrip(newRecord, v4);
    Assert.assertEquals(oldReaderRecord.get("batchGetLimit"), 500);
    Assert.assertNull(oldReaderRecord.getSchema().getField("multiKeyLongTailRetryThresholdsInMs"));
  }

  private GenericRecord roundTrip(GenericRecord record, Schema readerSchema) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
    new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
    encoder.flush();
    return new GenericDatumReader<GenericRecord>(record.getSchema(), readerSchema)
        .read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
  }

  @Test
  public void testMetadataResponseRecordCompatibility() throws InterruptedException {
    Map<Integer, Schema> schemaMap = initMetadataResponseRecordSchemaMap();
    Assert.assertFalse(schemaMap.isEmpty());
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
