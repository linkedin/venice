package com.linkedin.venice.system.store;

import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestStoreMetaValueSchemaCompatibility {
  private final Map<Integer, Schema> schemaVersionMap =
      Utils.getAllSchemasFromResources(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE);

  @Test
  public void testLatestSchemaCanReadOlderStoreMetadata() {
    int latestSchemaId = AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion();
    Schema latestSchema = schemaVersionMap.get(latestSchemaId);
    schemaVersionMap.forEach((schemaId, schema) -> {
      if (schemaId == latestSchemaId) {
        return;
      }
      SchemaCompatibility.SchemaPairCompatibility compatibility =
          SchemaCompatibility.checkReaderWriterCompatibility(latestSchema, schema);
      Assert.assertEquals(
          compatibility.getType(),
          SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
          "StoreMetaValue schema version " + schemaId + " is incompatible with latest schema version "
              + latestSchemaId);
    });
  }
}
