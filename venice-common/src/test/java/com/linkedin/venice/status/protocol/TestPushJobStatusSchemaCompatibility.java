package com.linkedin.venice.status.protocol;

import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPushJobStatusSchemaCompatibility {
  @Test
  public void testPushJobStatusValueSchemaCompatibility() throws IOException {
    Map<Integer, Schema> schemaVersionMap = new HashMap<>();
    int latestSchemaId = VeniceParentHelixAdmin.LATEST_PUSH_JOB_STATUS_VALUE_SCHEMA_ID;
    for (int i=1; i<= latestSchemaId; i++) {
      schemaVersionMap.put(i,
          Utils.getSchemaFromResource("avro/PushJobStatusRecord/PushJobStatusRecordValue/v"
              + i + "/PushJobStatusRecordValue.avsc"));
    }
    Schema latestSchema = schemaVersionMap.get(latestSchemaId);
    schemaVersionMap.forEach( (schemaId, schema) -> {
      if (schemaId == latestSchemaId) {
        return;
      }
      SchemaCompatibility.SchemaPairCompatibility backwardCompatibility =
          SchemaCompatibility.checkReaderWriterCompatibility(latestSchema, schema);
      Assert.assertEquals(backwardCompatibility.getType(), SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
          "PushJobStatusRecordValue schema version " + schemaId + " is incompatible with the latest schema "
      + "version of " + latestSchemaId);
    });
  }
}
