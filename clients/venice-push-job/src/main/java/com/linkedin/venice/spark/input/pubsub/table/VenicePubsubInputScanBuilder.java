package com.linkedin.venice.spark.input.pubsub.table;

import java.util.Properties;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;


public class VenicePubsubInputScanBuilder implements ScanBuilder {
  private final Properties jobConfig;

  public VenicePubsubInputScanBuilder(Properties properties) {
    this.jobConfig = properties;
  }

  @Override
  public Scan build() {
    return new VenicePubsubInputScan(jobConfig);
  }
}
