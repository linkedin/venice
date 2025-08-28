package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Row;


public class SparkPartitionWriter extends AbstractPartitionWriter {
  private static final Logger LOGGER = LogManager.getLogger(SparkPartitionWriter.class);

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
    List<VeniceRecordWithMetadata> valueRecordsForKey = new ArrayList<>();
    byte[] key = null;

    while (rows.hasNext()) {
      Row row = rows.next();
      byte[] incomingKey = Objects.requireNonNull(row.getAs(KEY_COLUMN_NAME), "Key cannot be null");

      byte[] rmd = null;
      try {
        rmd = row.getAs(RMD_COLUMN_NAME);
      } catch (IllegalArgumentException e) {
        // Ignore if timestamp is not present
      }

      if (!Arrays.equals(incomingKey, key)) {
        if (key != null) {
          // Key is different from the prev one and is not null. Write it out to PubSub.
          super.processValuesForKey(key, valueRecordsForKey.iterator(), dataWriterTaskTracker);
        }
        key = incomingKey;
        valueRecordsForKey = new ArrayList<>();
      }

      byte[] incomingValue = row.getAs(VALUE_COLUMN_NAME);
      valueRecordsForKey.add(new AbstractPartitionWriter.VeniceRecordWithMetadata(incomingValue, rmd));
    }

    if (key != null) {
      super.processValuesForKey(key, valueRecordsForKey.iterator(), dataWriterTaskTracker);
    }
  }
}
