package com.linkedin.venice.status.protocol;

import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPushJobDetailsSchemaCompatibility {
  @Test
  public void testPushJobStatusValueSchemaCompatibility() throws IOException {
    Map<Integer, Schema> schemaVersionMap = new HashMap<>();
    int latestSchemaId = 2;
    for (int i=1; i<= latestSchemaId; i++) {
      schemaVersionMap.put(i,
          Utils.getSchemaFromResource("avro/PushJobStatusRecord/PushJobDetails/v"
              + i + "/PushJobDetails.avsc"));
    }
    Schema latestSchema = schemaVersionMap.get(latestSchemaId);
    schemaVersionMap.forEach( (schemaId, schema) -> {
      if (schemaId == latestSchemaId) {
        return;
      }
      SchemaCompatibility.SchemaPairCompatibility backwardCompatibility =
          SchemaCompatibility.checkReaderWriterCompatibility(latestSchema, schema);
      Assert.assertEquals(backwardCompatibility.getType(), SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
          "PushJobDetails schema version " + schemaId + " is incompatible with the latest schema "
      + "version of " + latestSchemaId);
    });
  }
}
