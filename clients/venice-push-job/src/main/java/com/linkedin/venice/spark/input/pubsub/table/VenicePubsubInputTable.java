package com.linkedin.venice.spark.input.pubsub.table;

import static com.linkedin.venice.spark.SparkConstants.*;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;


/**
 * This is the entrypoint of the Pubsub input source. It is used by Spark to create a DataFrame from a Pubsub topic.
 */

public class VenicePubsubInputTable implements SupportsRead {
  static final String INPUT_TABLE_NAME = "venice_pubsub_table";
  private final VeniceProperties jobConfig;

  public VenicePubsubInputTable(VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
    //
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    Properties properties = jobConfig.getPropertiesCopy();
    properties.putAll(options.asCaseSensitiveMap());

    return new VenicePubsubInputScanBuilder(properties); // should we flip this to VeniceProperties?
  }

  @Override
  public String name() {
    return INPUT_TABLE_NAME;
  }

  @Override
  public StructType schema() {
    return KAFKA_INPUT_TABLE_SCHEMA;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.singleton(TableCapability.BATCH_READ);
  }
}
