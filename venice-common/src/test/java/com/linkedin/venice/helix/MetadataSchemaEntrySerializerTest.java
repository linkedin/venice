package com.linkedin.venice.helix;

import com.linkedin.venice.schema.MetadataSchemaEntry;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetadataSchemaEntrySerializerTest {
  @Test
  public void testDeserialize() {
    MetadataSchemaEntrySerializer serializer = new MetadataSchemaEntrySerializer();
    MetadataSchemaEntry
        entry = serializer.deserialize("{\"type\": \"int\"}".getBytes(), "/store/store1/metadata_schema/2-3");
    Assert.assertEquals(entry.getId(), 3);
    Assert.assertEquals(entry.getValueSchemaId(), 2);
  }
}
