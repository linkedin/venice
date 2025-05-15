package com.linkedin.venice.spark.input.table.rawPubSub;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;


public class VeniceRawPubsubInputPartitionReaderFactory implements PartitionReaderFactory {
  private static final long serialVersionUID = 1L;

  private final VeniceProperties jobConfig;

  public VeniceRawPubsubInputPartitionReaderFactory(VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {

    if (!(inputPartition instanceof VeniceBasicPubsubInputPartition)) {
      throw new IllegalArgumentException(
          "VeniceRawPubsubInputPartitionReaderFactory can only create readers for VeniceBasicPubsubInputPartition");
    }

    return new VeniceBasicPubsubInputPartitionReader(jobConfig, (VeniceBasicPubsubInputPartition) inputPartition);
  }

  // Make it explicit that this reader does not support columnar reads.
  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
