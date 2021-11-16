package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;


/**
 * This is simply a wrapper class to delegate the actual schema generation to version specific adapter.
 */

public class ReplicationMetadataSchemaGenerator {
  private static final ReplicationMetadataSchemaGeneratorV1 V1_REPLICATION_METADATA_SCHEMA_GENERATOR = new ReplicationMetadataSchemaGeneratorV1();

  private ReplicationMetadataSchemaGenerator() {}

  public static Schema generateMetadataSchema(String schemaStr, int version) {
    return generateMetadataSchema(Schema.parse(schemaStr), version);
  }

  public static Schema generateMetadataSchema(Schema schema, int version) {
    switch (version) {
      case 1:
        return V1_REPLICATION_METADATA_SCHEMA_GENERATOR.generateMetadataSchema(schema);
      default:
        throw new VeniceException("Unknown replication metadata version id");
    }
  }
}
