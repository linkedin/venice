package com.linkedin.venice.spark.input.hdfs;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;


public class VeniceHdfsInputScanBuilder implements ScanBuilder {
  private final VeniceProperties jobConfig;

  public VeniceHdfsInputScanBuilder(VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public Scan build() {
    return new VeniceHdfsInputScan(jobConfig);
  }
}
