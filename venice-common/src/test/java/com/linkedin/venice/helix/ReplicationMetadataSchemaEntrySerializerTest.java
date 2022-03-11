package com.linkedin.venice.helix;

import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ReplicationMetadataSchemaEntrySerializerTest {
  @Test
  public void testDeserialize() {
    ReplicationMetadataSchemaEntrySerializer serializer = new ReplicationMetadataSchemaEntrySerializer();
    ReplicationMetadataSchemaEntry
        entry = serializer.deserialize("{\"type\": \"int\"}".getBytes(), "/store/store1/timestamp-metadata-schema/2-3");
    Assert.assertEquals(entry.getId(), 3);
    Assert.assertEquals(entry.getValueSchemaID(), 2);
  }
}
