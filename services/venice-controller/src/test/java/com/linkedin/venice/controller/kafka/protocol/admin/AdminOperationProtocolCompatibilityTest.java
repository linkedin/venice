package com.linkedin.venice.controller.kafka.protocol.admin;

import com.linkedin.venice.controller.kafka.protocol.ProtocolCompatibilityTest;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class AdminOperationProtocolCompatibilityTest extends ProtocolCompatibilityTest {
  @Test
  public void testAdminOperationProtocolCompatibility() throws InterruptedException {
    Map<Integer, Schema> schemaMap = AdminOperationSerializer.initProtocolMap();
    int latestSchemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;
    testProtocolCompatibility(schemaMap, latestSchemaId);
  }
}
