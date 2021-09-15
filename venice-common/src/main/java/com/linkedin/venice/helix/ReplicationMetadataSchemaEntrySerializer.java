package com.linkedin.venice.helix;

import com.linkedin.venice.schema.ReplicationMetadataSchemaEntry;


/**
 * This class is registered in ZKClient to handle Replication metadata schema serialization.
 * The path is like: /store/${store_name}/timestamp-metadata-schema/${value_schema_id}-${replication_metadata_version_id}
 */
public class ReplicationMetadataSchemaEntrySerializer extends AbstractSchemaEntrySerializer<ReplicationMetadataSchemaEntry> {
  @Override
  protected ReplicationMetadataSchemaEntry getInstance(int schemaVersion, int protocolVersion, byte[] schemaBytes) {
    return new ReplicationMetadataSchemaEntry(schemaVersion, protocolVersion, schemaBytes);
  }
}