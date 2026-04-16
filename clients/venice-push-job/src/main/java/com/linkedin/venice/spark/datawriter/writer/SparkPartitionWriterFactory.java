package com.linkedin.venice.spark.datawriter.writer;

import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;


public class SparkPartitionWriterFactory implements MapPartitionsFunction<Row, Row> {
  private static final long serialVersionUID = 1L;
  private final Broadcast<Properties> jobProps;
  private final DataWriterAccumulators accumulators;

  public SparkPartitionWriterFactory(Broadcast<Properties> jobProps, DataWriterAccumulators accumulators) {
    this.jobProps = jobProps;
    this.accumulators = accumulators;
  }

  @Override
  public Iterator<Row> call(Iterator<Row> rows) throws Exception {
    int partitionId;
    long recordCount;
    try (SparkPartitionWriter partitionWriter = new SparkPartitionWriter(jobProps.getValue(), accumulators)) {
      partitionWriter.processRows(rows);
      partitionId = partitionWriter.getPartitionId();
      recordCount = partitionWriter.getRecordCount();
    }
    return Collections.singletonList(RowFactory.create(partitionId, recordCount)).iterator();
  }
}
