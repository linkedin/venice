package com.linkedin.venice.helix;

import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RmdSchemaEntrySerializerTest {
  @Test
  public void testDeserialize() {
    ReplicationMetadataSchemaEntrySerializer serializer = new ReplicationMetadataSchemaEntrySerializer();
    RmdSchemaEntry entry =
        serializer.deserialize("{\"type\": \"int\"}".getBytes(), "/store/store1/timestamp-metadata-schema/2-3");
    Assert.assertEquals(entry.getId(), 3);
    Assert.assertEquals(entry.getValueSchemaID(), 2);
  }
}
