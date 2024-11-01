package com.linkedin.venice.spark.input.pubsub.table;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;


public class VenicePubsubInputScanBuilder implements ScanBuilder {
  private final VeniceProperties jobConfig;

  public VenicePubsubInputScanBuilder(VeniceProperties properties) {
    this.jobConfig = properties;
  }

  @Override
  public Scan build() {
    return new VenicePubsubInputScan(jobConfig);
  }
}
