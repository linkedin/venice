package com.linkedin.venice.hadoop.spark.datawriter.recordprocessor;

import com.linkedin.venice.hadoop.spark.datawriter.task.DataWriterAccumulators;
import java.util.Iterator;
import java.util.Properties;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;


public class SparkInputRecordProcessorFactory implements FlatMapFunction<Row, Row> {
  private static final long serialVersionUID = 1L;
  private transient SparkInputRecordProcessor processor = null;
  private final Broadcast<Properties> jobProps;
  private final DataWriterAccumulators accumulators;

  public SparkInputRecordProcessorFactory(Broadcast<Properties> jobProps, DataWriterAccumulators accumulators) {
    this.jobProps = jobProps;
    this.accumulators = accumulators;
  }

  @Override
  public Iterator<Row> call(Row row) throws Exception {
    if (processor == null) {
      processor = new SparkInputRecordProcessor(jobProps.getValue(), accumulators);
    }

    return processor.processRecord(row);
  }
}
