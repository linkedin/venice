package com.linkedin.venice.spark.input.pubsub.table;

import java.util.Properties;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;


public class VenicePubsubInputPartitionReaderFactory implements PartitionReaderFactory {
  private static final long serialVersionUID = 1L;

  private final Properties jobConfig;

  public VenicePubsubInputPartitionReaderFactory(Properties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {

    if (!(inputPartition instanceof VenicePubsubInputPartition)) {
      throw new IllegalArgumentException(
          "VenicePubsubInputPartitionReaderFactory can only create readers for VenicePubsubInputPartition");
    }

    return new VenicePubsubInputPartitionReader(jobConfig, (VenicePubsubInputPartition) inputPartition);
  }

  // Make it explicit that this reader does not support columnar reads.
  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
