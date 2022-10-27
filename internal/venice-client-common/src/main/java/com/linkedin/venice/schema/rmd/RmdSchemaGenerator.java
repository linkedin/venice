package com.linkedin.venice.schema.rmd;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.v1.RmdSchemaGeneratorV1;
import io.tehuti.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * This is simply a wrapper class to delegate the actual schema generation to version specific adapter.
 */

public class RmdSchemaGenerator {
  private static final int GENERATOR_V1 = 1;
  private static final int LATEST_VERSION = GENERATOR_V1;
  // It's fine to use V1 object in the map as V2 extends from V1. We'll need to abstract
  // a new generator in the future if we bring some incompatible changes to the generator. (
  // in case a newer adapter cannot extend from the older ones.)
  private static final Map<Integer, RmdSchemaGeneratorV1> RMD_SCHEMA_GENERATOR;

  static {
    Map<Integer, RmdSchemaGeneratorV1> tmpMap = new HashMap<>(LATEST_VERSION);
    tmpMap.put(GENERATOR_V1, new RmdSchemaGeneratorV1());
    RMD_SCHEMA_GENERATOR = Collections.unmodifiableMap(tmpMap);
  }

  private RmdSchemaGenerator() {
  }

  public static Schema generateMetadataSchema(String schemaStr, int version) {
    return generateMetadataSchema(AvroCompatibilityHelper.parse(schemaStr), version);
  }

  /**
   * Generate the latest replication metadata schema.
   *
   * @param schema source schema from which replication metadata schema is generated
   * @return Generated replication metadata schema
   */
  public static Schema generateMetadataSchema(Schema schema) {
    return generateMetadataSchema(schema, LATEST_VERSION);
  }

  public static int getLatestVersion() {
    return LATEST_VERSION;
  }

  public static Schema generateMetadataSchema(Schema schema, int version) {
    Utils.notNull(schema);
    RmdSchemaGeneratorV1 metadataSchemaGenerator = RMD_SCHEMA_GENERATOR.get(version);
    if (metadataSchemaGenerator == null) {
      throw new VeniceException("Unknown replication metadata version id: " + version);
    }
    return metadataSchemaGenerator.generateMetadataSchema(schema);
  }
}
