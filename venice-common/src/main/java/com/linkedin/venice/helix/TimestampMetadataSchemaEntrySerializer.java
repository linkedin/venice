package com.linkedin.venice.helix;

import com.linkedin.venice.schema.ReplicationMetadataSchemaEntry;


/**
 * This class is registered in ZKClient to handle Timestamp metadata schema serialization.
 * The path is like: /store/${store_name}/timestamp-metadata-schema/${value_schema_id}-${timestamp_metadata_version_id}
 */
public class TimestampMetadataSchemaEntrySerializer extends AbstractSchemaEntrySerializer<ReplicationMetadataSchemaEntry> {
  @Override
  protected ReplicationMetadataSchemaEntry getInstance(int schemaVersion, int protocolVersion, byte[] schemaBytes) {
    return new ReplicationMetadataSchemaEntry(schemaVersion, protocolVersion, schemaBytes);
  }
}