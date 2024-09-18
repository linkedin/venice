package com.linkedin.venice.spark.datawriter.recordprocessor;

import com.linkedin.venice.hadoop.input.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.hadoop.input.recordreader.avro.IdentityVeniceRecordReader;
import com.linkedin.venice.hadoop.task.datawriter.AbstractInputRecordProcessor;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.spark.SparkConstants;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.task.SparkDataWriterTaskTracker;
import com.linkedin.venice.spark.engine.SparkEngineTaskConfigProvider;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;


/**
 * An implementation of {@link AbstractInputRecordProcessor} for Spark that processes input records from the dataframe
 * and emits an {@link Iterator} of {@link Row} with {@link DEFAULT_SCHEMA} as the schema.
 */
public class SparkInputRecordProcessor extends AbstractInputRecordProcessor<ByteBuffer, ByteBuffer> {
  private final DataWriterTaskTracker dataWriterTaskTracker;

  public SparkInputRecordProcessor(Properties jobProperties, DataWriterAccumulators accumulators) {
    dataWriterTaskTracker = new SparkDataWriterTaskTracker(accumulators);
    super.configure(new SparkEngineTaskConfigProvider(jobProperties));
  }

  public Iterator<Row> processRecord(Row record) {
    List<Row> outputRows = new ArrayList<>();
    ByteBuffer keyBB = ByteBuffer.wrap(record.getAs(SparkConstants.KEY_COLUMN_NAME));
    byte[] value = record.getAs(SparkConstants.VALUE_COLUMN_NAME);
    ByteBuffer valueBB = value == null ? null : ByteBuffer.wrap(value);
    super.processRecord(keyBB, valueBB, getRecordEmitter(outputRows), dataWriterTaskTracker);
    return outputRows.iterator();
  }

  @Override
  protected AbstractVeniceRecordReader<ByteBuffer, ByteBuffer> getRecordReader(VeniceProperties props) {
    return IdentityVeniceRecordReader.getInstance();
  }

  private BiConsumer<byte[], byte[]> getRecordEmitter(List<Row> rows) {
    return (key, value) -> {
      rows.add(new GenericRowWithSchema(new Object[] { key, value }, SparkConstants.DEFAULT_SCHEMA));
    };
  }
}
