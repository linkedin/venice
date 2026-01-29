package com.linkedin.venice.spark.input.pubsub.raw;

import com.linkedin.venice.spark.input.pubsub.SparkPubSubInputFormat;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;


public class VeniceRawPubsubInputScanBuilder implements ScanBuilder {
  private final VeniceProperties jobConfig;

  public VeniceRawPubsubInputScanBuilder(VeniceProperties properties) {
    this.jobConfig = properties;
  }

  @Override
  public Scan build() {
    return new SparkPubSubInputFormat(jobConfig);
  }
}
