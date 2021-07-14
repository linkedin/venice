package com.linkedin.venice.helix;

import com.linkedin.venice.schema.TimestampMetadataSchemaEntry;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TimestampMetadataSchemaEntrySerializerTest {
  @Test
  public void testDeserialize() {
    TimestampMetadataSchemaEntrySerializer serializer = new TimestampMetadataSchemaEntrySerializer();
    TimestampMetadataSchemaEntry
        entry = serializer.deserialize("{\"type\": \"int\"}".getBytes(), "/store/store1/timestamp-metadata-schema/2-3");
    Assert.assertEquals(entry.getId(), 3);
    Assert.assertEquals(entry.getValueSchemaId(), 2);
  }
}
