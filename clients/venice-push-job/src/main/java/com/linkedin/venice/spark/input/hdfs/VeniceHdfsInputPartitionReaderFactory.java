package com.linkedin.venice.spark.input.hdfs;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;


public class VeniceHdfsInputPartitionReaderFactory implements PartitionReaderFactory {
  private static final long serialVersionUID = 1L;

  private final VeniceProperties jobConfig;

  public VeniceHdfsInputPartitionReaderFactory(VeniceProperties jobConfig) {
    this.jobConfig = jobConfig;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    if (!(partition instanceof VeniceHdfsInputPartition)) {
      throw new IllegalArgumentException(
          "VeniceHdfsInputPartitionReaderFactory can only create readers for VeniceHdfsInputPartition");
    }

    return new VeniceHdfsInputPartitionReader(jobConfig, (VeniceHdfsInputPartition) partition);
  }
}
