package com.linkedin.venice.spark.input.pubsub.raw;

import static com.linkedin.venice.spark.SparkConstants.BASIC_RAW_PUBSUB_INPUT_TABLE_SCHEMA;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;


public class VeniceRawPubsubInputTable implements SupportsRead {
  static final String INPUT_TABLE_NAME = "venice_raw_pubsub_table";
  private final VeniceProperties jobConfig;

  public VeniceRawPubsubInputTable(VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    Properties properties = jobConfig.getPropertiesCopy();
    properties.putAll(options.asCaseSensitiveMap());

    return new VeniceRawPubsubInputScanBuilder(new VeniceProperties(properties));
  }

  @Override
  public String name() {
    return INPUT_TABLE_NAME;
  }

  @Override
  public StructType schema() {
    return BASIC_RAW_PUBSUB_INPUT_TABLE_SCHEMA;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.singleton(TableCapability.BATCH_READ);
  }
}
