package com.linkedin.venice.schema.rmd;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.v1.ReplicationMetadataSchemaGeneratorV1;
import com.linkedin.venice.schema.rmd.v2.ReplicationMetadataSchemaGeneratorV2;
import io.tehuti.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * This is simply a wrapper class to delegate the actual schema generation to version specific adapter.
 */

public class ReplicationMetadataSchemaGenerator {
  private static final int LATEST_VERSION = 2;
  // It's fine to use V1 object in the map as V2 extends from V1. We'll need to abstract
  // a new generator in the future if we bring some incompatible changes to the generator. (
  // in case a newer adapter cannot extend from the older ones.)
  private static final Map<Integer, ReplicationMetadataSchemaGeneratorV1> REPLICATION_METADATA_SCHEMA_GENERATOR;

  static {
    Map<Integer, ReplicationMetadataSchemaGeneratorV1> tmpMap = new HashMap<>(LATEST_VERSION);
    tmpMap.put(1, new ReplicationMetadataSchemaGeneratorV1());
    tmpMap.put(2, new ReplicationMetadataSchemaGeneratorV2());
    REPLICATION_METADATA_SCHEMA_GENERATOR = Collections.unmodifiableMap(tmpMap);
  }

  private ReplicationMetadataSchemaGenerator() {}

  public static Schema generateMetadataSchema(String schemaStr, int version) {
    return generateMetadataSchema(Schema.parse(schemaStr), version);
  }

  public static Schema generateMetadataSchema(Schema schema) {
    return generateMetadataSchema(schema, LATEST_VERSION);
  }

  public static Schema generateMetadataSchema(Schema schema, int version) {
    Utils.notNull(schema);
    ReplicationMetadataSchemaGeneratorV1 metadataSchemaGenerator = REPLICATION_METADATA_SCHEMA_GENERATOR.get(version);
    if (metadataSchemaGenerator == null) {
      throw new VeniceException("Unknown replication metadata version id: " + version);
    }
    return metadataSchemaGenerator.generateMetadataSchema(schema);
  }
}
