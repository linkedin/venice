package com.linkedin.venice.status.protocol;

import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPushJobDetailsSchemaCompatibility {
  private Map<Integer, Schema> schemaVersionMap =
      Utils.getAllSchemasFromResources(AvroProtocolDefinition.PUSH_JOB_DETAILS);

  @Test
  public void testPushJobStatusValueSchemaCompatibility() {
    Assert.assertTrue(AvroProtocolDefinition.PUSH_JOB_DETAILS.currentProtocolVersion.isPresent());
    int latestSchemaId = AvroProtocolDefinition.PUSH_JOB_DETAILS.currentProtocolVersion.get();
    Schema latestSchema = schemaVersionMap.get(latestSchemaId);
    schemaVersionMap.forEach((schemaId, schema) -> {
      if (schemaId == latestSchemaId) {
        return;
      }
      SchemaCompatibility.SchemaPairCompatibility backwardCompatibility =
          SchemaCompatibility.checkReaderWriterCompatibility(latestSchema, schema);
      Assert.assertEquals(
          backwardCompatibility.getType(),
          SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
          "PushJobDetails schema version " + schemaId + " is incompatible with the latest schema " + "version of "
              + latestSchemaId);
    });
  }
}
