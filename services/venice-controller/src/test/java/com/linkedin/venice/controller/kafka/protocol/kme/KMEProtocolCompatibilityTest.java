package com.linkedin.venice.controller.kafka.protocol.kme;

import com.linkedin.venice.controller.kafka.protocol.ProtocolCompatibilityTest;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class KMEProtocolCompatibilityTest extends ProtocolCompatibilityTest {
  @Test
  public void testKMEProtocolCompatibility() throws InterruptedException {
    Map<Integer, Schema> schemaMap = initKMEProtocolMap();
    int latestSchemaId = schemaMap.size();
    testProtocolCompatibility(schemaMap, latestSchemaId);
  }

  private Map<Integer, Schema> initKMEProtocolMap() {
    try {
      Map<Integer, Schema> protocolSchemaMap = new HashMap<>();
      int knownKMEProtocolsNum = new KafkaValueSerializer().knownProtocols().size();
      for (int i = 1; i <= knownKMEProtocolsNum; i++) {
        protocolSchemaMap
            .put(i, Utils.getSchemaFromResource("avro/KafkaMessageEnvelope/v" + i + "/KafkaMessageEnvelope.avsc"));
      }
      return protocolSchemaMap;
    } catch (IOException e) {
      throw new VeniceMessageException("Could not initialize " + KafkaValueSerializer.class.getSimpleName(), e);
    }
  }
}
