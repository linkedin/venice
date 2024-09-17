package com.linkedin.venice.spark.input;

import com.linkedin.venice.hadoop.input.recordreader.VeniceRecordIterator;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;


public abstract class VeniceAbstractPartitionReader implements PartitionReader<InternalRow> {
  private final VeniceRecordIterator recordIterator;

  public VeniceAbstractPartitionReader(VeniceProperties jobConfig, InputPartition partition) {
    this.recordIterator = createRecordIterator(jobConfig, partition);
  }

  protected abstract VeniceRecordIterator createRecordIterator(VeniceProperties jobConfig, InputPartition partition);

  @Override
  public boolean next() throws IOException {
    return recordIterator.next();
  }

  @Override
  public InternalRow get() {
    return new GenericInternalRow(new Object[] { recordIterator.getCurrentKey(), recordIterator.getCurrentValue() });
  }

  @Override
  public void close() throws IOException {
    Utils.closeQuietlyWithErrorLogged(recordIterator);
  }
}
