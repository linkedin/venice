package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.schema.SchemaEntry;
import java.io.IOException;


/**
 * This class to use to serialize/deserialize Zookeeper node
 */
public class SchemaEntrySerializer implements VeniceSerializer<SchemaEntry> {
  /**
   * This function only serialize schema content, and the caller will be charge of
   * storing schema id as part of file path.
   *
   * @param object
   * @param path
   * @return
   * @throws IOException
   */
  @Override
  public byte[] serialize(SchemaEntry object, String path) throws IOException {
    return object.getSchemaBytes();
  }

  /**
   * This function will extract schema from file path, and schema content from ZNode data.
   *
   * @param bytes
   * @param path
   * @return
   * @throws IOException
   */
  @Override
  public SchemaEntry deserialize(byte[] bytes, String path) throws IOException {
    if (path.isEmpty()) {
      throw new VeniceException("File path for schema is empty");
    }
    // Get schema id from path
    String[] paths = path.split("/");
    String schemaId = paths[paths.length - 1];
    return new SchemaEntry(Integer.parseInt(schemaId), bytes);
  }
}
