package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.schema.DerivedSchemaEntry;

import static com.linkedin.venice.helix.HelixSchemaAccessor.*;


/**
 * This class is registered in ZKClient to handle derive schema serialization.
 * The path is like: /store/${store_name}/derived_schema/${value_schema_id}_${derived_schema_id}
 */
public class DerivedSchemaEntrySerializer implements VeniceSerializer<DerivedSchemaEntry> {
  @Override
  public byte[] serialize(DerivedSchemaEntry object, String path) {
    return object.getSchemaBytes();
  }

  @Override
  public DerivedSchemaEntry deserialize(byte[] bytes, String path) {
    if (path.isEmpty()) {
      throw new VeniceException("File path for schema is empty");
    }
    // Get schema id from path
    String[] paths = path.split("/");
    String [] ids = paths[paths.length - 1].split(DERIVED_SCHEMA_DELIMITER);
    return new DerivedSchemaEntry(Integer.parseInt(ids[0]), Integer.parseInt(ids[1]), bytes);
  }
}
