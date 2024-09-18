package com.linkedin.venice.spark.input.hdfs;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;

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
 * A table format that is used by Spark to read Avro files from HDFS for use in VenicePushJob.
 */
public class VeniceHdfsInputTable implements SupportsRead {
  private final VeniceProperties jobConfig;
  // Visible for testing
  static final String INPUT_TABLE_NAME = "venice_hdfs_table";

  public VeniceHdfsInputTable(VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    Properties properties = jobConfig.getPropertiesCopy();
    properties.putAll(options.asCaseSensitiveMap());
    return new VeniceHdfsInputScanBuilder(new VeniceProperties(properties));
  }

  @Override
  public String name() {
    return INPUT_TABLE_NAME;
  }

  @Override
  public StructType schema() {
    return DEFAULT_SCHEMA;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.singleton(TableCapability.BATCH_READ);
  }
}
