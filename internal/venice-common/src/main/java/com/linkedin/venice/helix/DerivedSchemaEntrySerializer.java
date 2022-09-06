package com.linkedin.venice.helix;

import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;


/**
 * This class is registered in ZKClient to handle derive schema serialization.
 * The path is like: /store/${store_name}/derived_schema/${value_schema_id}_${derived_schema_id}
 */
public class DerivedSchemaEntrySerializer extends AbstractSchemaEntrySerializer<DerivedSchemaEntry> {
  @Override
  protected DerivedSchemaEntry getInstance(int schemaVersion, int protocolVersion, byte[] schemaBytes) {
    return new DerivedSchemaEntry(schemaVersion, protocolVersion, schemaBytes);
  }
}
