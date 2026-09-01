package com.linkedin.venice.spark.datawriter.writer;

import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.JavaConverters;


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
    SparkPartitionWriter partitionWriter = new SparkPartitionWriter(jobProps.getValue(), accumulators);
    long recordCount;
    try (SparkPartitionWriter ignored = partitionWriter) {
      partitionWriter.processRows(rows);
      recordCount = partitionWriter.getRecordCount();
    }
    // Read these after close so the task output reflects any regions disabled while flushing or closing the
    // writer, and includes the time the flush/close themselves took.
    ArrayList<String> failedExternalStorageRegions = new ArrayList<>(partitionWriter.getFailedExternalStorageRegions());
    Collections.sort(failedExternalStorageRegions);
    long externalStorageWriteTimeMs = partitionWriter.getExternalStorageWriteTimeMs();
    long veniceWriteTimeMs = partitionWriter.getVeniceWriteTimeMs();
    int partitionId = TaskContext.get().partitionId();
    return Collections.singletonList(
        RowFactory.create(
            partitionId,
            recordCount,
            JavaConverters.asScalaBuffer(failedExternalStorageRegions).toSeq(),
            externalStorageWriteTimeMs,
            veniceWriteTimeMs))
        .iterator();
  }
}
