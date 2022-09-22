package com.linkedin.venice.helix;

import static com.linkedin.venice.helix.HelixSchemaAccessor.MULTIPART_SCHEMA_VERSION_DELIMITER;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.schema.GeneratedSchemaEntry;


public abstract class AbstractSchemaEntrySerializer<T extends GeneratedSchemaEntry> implements VeniceSerializer<T> {
  protected abstract T getInstance(int schemaVersion, int protocolVersion, byte[] schemaBytes);

  @Override
  public byte[] serialize(T object, String path) {
    return object.getSchemaBytes();
  }

  @Override
  public T deserialize(byte[] bytes, String path) {
    if (path.isEmpty()) {
      throw new VeniceException("File path for schema is empty");
    }
    // Get schema id from path
    int indexOfLastPathElement = path.lastIndexOf("/");
    if (indexOfLastPathElement == -1) {
      throw new IllegalArgumentException("Invalid path param: " + path);
    }
    String lastPathElement = path.substring(indexOfLastPathElement + 1);
    String[] ids = lastPathElement.split(MULTIPART_SCHEMA_VERSION_DELIMITER);
    return getInstance(Integer.parseInt(ids[0]), Integer.parseInt(ids[1]), bytes);
  }
}
