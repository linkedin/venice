package com.linkedin.venice.serialization.avro;

import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.SERVER_ADMIN_RESPONSE;

import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroProtocolDefinitionTest {
  @Test
  public void testGetSerializer() {
    Assert.assertNotNull(KAFKA_MESSAGE_ENVELOPE.getSerializer());
    Assert.assertNotNull(SERVER_ADMIN_RESPONSE.getSerializer());
  }

  @Test
  public void testBackwardCompatibility() {
    int latestSchemId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;
    Schema lastestSchema = AdminOperationSerializer.getSchema(latestSchemId);

    for (int previousSchemaId = 1; previousSchemaId < latestSchemId; previousSchemaId++) {
      Schema previousSchema = AdminOperationSerializer.getSchema(previousSchemaId);
      SchemaCompatibility.SchemaPairCompatibility compatibility =
          SchemaCompatibility.checkReaderWriterCompatibility(lastestSchema, previousSchema);

      // Assert that the schemas are compatible
      Assert.assertTrue(
          compatibility.getType() == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
          "Schemas are not backward compatible: " + compatibility.getDescription());
    }
  }
}
