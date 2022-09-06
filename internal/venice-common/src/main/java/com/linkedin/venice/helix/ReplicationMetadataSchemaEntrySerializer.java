package com.linkedin.venice.helix;

import com.linkedin.venice.schema.rmd.RmdSchemaEntry;


/**
 * This class is registered in ZKClient to handle Replication metadata schema serialization.
 * The path is like: /store/${store_name}/timestamp-metadata-schema/${value_schema_id}-${replication_metadata_version_id}
 */
public class ReplicationMetadataSchemaEntrySerializer extends AbstractSchemaEntrySerializer<RmdSchemaEntry> {
  @Override
  protected RmdSchemaEntry getInstance(int schemaVersion, int protocolVersion, byte[] schemaBytes) {
    return new RmdSchemaEntry(schemaVersion, protocolVersion, schemaBytes);
  }
}
