package com.linkedin.venice.helix;

import com.linkedin.venice.schema.MetadataSchemaEntry;


/**
 * This class is registered in ZKClient to handle Metadata schema serialization.
 * The path is like: /store/${store_name}/metadata_schema/${value_schema_id}_${metadata_version_id}
 */
public class MetadataSchemaEntrySerializer extends AbstractSchemaEntrySerializer<MetadataSchemaEntry> {
  @Override
  protected MetadataSchemaEntry getInstance(int schemaVersion, int protocolVersion, byte[] schemaBytes) {
    return new MetadataSchemaEntry(schemaVersion, protocolVersion, schemaBytes);
  }
}