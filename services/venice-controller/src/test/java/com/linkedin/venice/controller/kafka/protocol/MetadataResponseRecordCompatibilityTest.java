package com.linkedin.venice.controller.kafka.protocol;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetadataResponseRecordCompatibilityTest extends ProtocolCompatibilityTest {
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
