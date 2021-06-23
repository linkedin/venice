package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.MetadataSchemaAdapterV1;
import org.apache.avro.Schema;


/**
 * This is simply a wrapper class to delegate the actual schema generation to version specific adapter.
 */

public class MetadataSchemaAdapter {
  private MetadataSchemaAdapter() {}

  public static Schema parse(String schemaStr, int version) {
    return parse(Schema.parse(schemaStr), version);
  }

  public static Schema parse(Schema schema, int version) {
    switch (version) {
      case 1:
        return new MetadataSchemaAdapterV1().parse(schema);
      default:
        throw new VeniceException("Unknown metadata version id");
    }
  }
}
