package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_VERSION_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;

import com.linkedin.venice.hadoop.task.datawriter.AbstractPartitionWriter;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.task.SparkDataWriterTaskTracker;
import com.linkedin.venice.spark.engine.SparkEngineTaskConfigProvider;
import com.linkedin.venice.spark.utils.SparkScalaUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;


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

    StructType schema = null;
    int rmdIdx = -1;
    int schemaIdIdx = -1;
    int rmdVersionIdIdx = -1;
    boolean indicesResolved = false;

    while (rows.hasNext()) {
      Row row = rows.next();

      if (!indicesResolved) {
        schema = row.schema();
        rmdIdx = SparkScalaUtils.getFieldIndex(schema, RMD_COLUMN_NAME);
        schemaIdIdx = SparkScalaUtils.getFieldIndex(schema, SCHEMA_ID_COLUMN_NAME);
        rmdVersionIdIdx = SparkScalaUtils.getFieldIndex(schema, RMD_VERSION_ID_COLUMN_NAME);
        indicesResolved = true;
      }

      byte[] incomingKey = Objects.requireNonNull(row.getAs(KEY_COLUMN_NAME), "Key cannot be null");

      byte[] rmd = rmdIdx >= 0 ? row.getAs(rmdIdx) : null;

      if (!Arrays.equals(incomingKey, key)) {
        if (key != null) {
          // Key is different from the prev one and is not null. Write it out to PubSub.
          super.processValuesForKey(key, valueRecordsForKey.iterator(), dataWriterTaskTracker);
        }
        key = incomingKey;
        valueRecordsForKey = new ArrayList<>();
      }

      int schemaId = schemaIdIdx >= 0 ? row.getAs(schemaIdIdx) : -1;
      int rmdVersionId = rmdVersionIdIdx >= 0 ? row.getAs(rmdVersionIdIdx) : -1;

      byte[] incomingValue = row.getAs(VALUE_COLUMN_NAME);
      valueRecordsForKey
          .add(new AbstractPartitionWriter.VeniceRecordWithMetadata(incomingValue, rmd, schemaId, rmdVersionId));
    }

    if (key != null) {
      super.processValuesForKey(key, valueRecordsForKey.iterator(), dataWriterTaskTracker);
    }
  }
}
