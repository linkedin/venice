package com.linkedin.venice.controller.kafka.protocol.admin;

import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.schema.avro.SchemaCompatibility;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdminOperationProtocolCompatibilityTest {
  @Test
  public void testAdminOperationProtocolCompatibility() {
    Map<Integer, Schema> schemaMap = AdminOperationSerializer.initProtocolMap();
    int latestSchemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;

    Assert.assertNotNull(schemaMap.containsKey(latestSchemaId), "The latest schema should exist!");

    Schema latestSchema = schemaMap.get(latestSchemaId);
    schemaMap.forEach( (schemaId, schema) -> {
      if (schemaId == latestSchemaId) {
        return;
      }
      SchemaCompatibility.SchemaPairCompatibility backwardCompatibility =
          SchemaCompatibility.checkReaderWriterCompatibility(latestSchema, schema);
      Assert.assertEquals(backwardCompatibility.getType(), SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
          "Older admin operation protocol with schema id: " + schemaId + ", schema: " + schema.toString(true)
              + " is not compatible with the latest admin operation protocol with schema id: " + latestSchemaId
              + ", schema: " + latestSchema.toString(true));
    });
  }
}
