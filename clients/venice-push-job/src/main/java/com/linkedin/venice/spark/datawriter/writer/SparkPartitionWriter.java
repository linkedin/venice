package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;

import com.linkedin.venice.hadoop.task.datawriter.AbstractPartitionWriter;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.task.SparkDataWriterTaskTracker;
import com.linkedin.venice.spark.engine.SparkEngineTaskConfigProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.spark.sql.Row;


public class SparkPartitionWriter extends AbstractPartitionWriter {
  private final SparkDataWriterTaskTracker dataWriterTaskTracker;

  public SparkPartitionWriter(Properties jobProperties, DataWriterAccumulators accumulators) {
    dataWriterTaskTracker = new SparkDataWriterTaskTracker(accumulators);
    super.configure(new SparkEngineTaskConfigProvider(jobProperties));
  }

  @Override
  protected long getTotalIncomingDataSizeInBytes() {
    // TODO: Explore if this info can be fetched from Spark engine or if it can be passed down via the DAG
    return super.getTotalIncomingDataSizeInBytes();
  }

  void processRows(Iterator<Row> rows) {
    byte[] key = null;
    List<byte[]> valuesForKey = null;
    while (rows.hasNext()) {
      Row row = rows.next();
      byte[] incomingKey = Objects.requireNonNull(row.getAs(KEY_COLUMN_NAME), "Key cannot be null");

      if (!Arrays.equals(incomingKey, key)) {
        if (key != null) {
          // Key is different from the prev one and is not null. Write it out to PubSub.
          super.processValuesForKey(key, valuesForKey.iterator(), dataWriterTaskTracker);
        }
        key = incomingKey;
        valuesForKey = new ArrayList<>();
      }

      byte[] incomingValue = row.getAs(VALUE_COLUMN_NAME);
      valuesForKey.add(incomingValue);
    }

    if (key != null) {
      super.processValuesForKey(key, valuesForKey.iterator(), dataWriterTaskTracker);
    }
  }
}
