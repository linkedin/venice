package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import org.apache.log4j.Logger;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.hadoop.MapReduceConstants.*;


/**
 * An abstraction of the mapper that would return serialized Avro key/value pairs.
 * The class read config {@link KafkaPushJob#VENICE_MAP_ONLY} to determine whether
 * it will send the output to reducer or Kafka straightly.
 *
 * @param <INPUT_KEY> type of the input key read from InputFormat
 * @param <INPUT_VALUE> type of the input value read from InputFormat
 */

public abstract class AbstractVeniceMapper<INPUT_KEY, INPUT_VALUE>
    implements Mapper<INPUT_KEY, INPUT_VALUE, BytesWritable, BytesWritable> {
  private static final Logger LOGGER = Logger.getLogger(AbstractVeniceMapper.class);

  private boolean isMapperOnly;
  private VeniceReducer reducer = null;

  protected AbstractVeniceRecordReader<INPUT_KEY, INPUT_VALUE> veniceRecordReader;

  @Override
  public void map(INPUT_KEY inputKey, INPUT_VALUE inputValue, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
      throws IOException {
    byte[] recordKey = veniceRecordReader.getKeyBytes(inputKey, inputValue);
    byte[] recordValue = veniceRecordReader.getValueBytes(inputKey, inputValue);

    if (recordKey == null) {
      throw new VeniceException("Mapper received a empty key record");
    }

    if (recordValue == null) {
      LOGGER.warn("Received null record, skip.");
      if (reporter != null) {
        reporter.incrCounter(COUNTER_GROUP_KAFKA, EMPTY_RECORD, 1);
      }
      return;
    }

    if (isMapperOnly) {
      reducer.sendMessageToKafka(recordKey, recordValue, reporter);
      return;
    }

    if (reporter != null) {
      reporter.incrCounter(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_KEY_SIZE, recordKey.length);
      reporter.incrCounter(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_VALUE_SIZE, recordValue.length);
    }

    output.collect(new BytesWritable(recordKey), new BytesWritable(recordValue));
  }

  /**
   * Return a AbstractVeniceRecordReader which will be used to handle all input operations
   */
  AbstractVeniceRecordReader getRecordReader() {
    if (veniceRecordReader == null) {
      LOGGER.warn("venice record reader has not been initialized yet. Please call configure().");
    }

    return veniceRecordReader;
  }

  /**
   * An method for child classes to setup record reader and anything custom. It will be called before
   * {@link #configure(VeniceProperties)}
   */
  abstract protected void configure(VeniceProperties props);

  @Override
  public void close() throws IOException {
    //no-op
  }

  @Override
  public void configure(JobConf job) {
    VeniceProperties props = HadoopUtils.getVeniceProps(job);

    configure(props);

    if (this.veniceRecordReader == null) {
      throw new VeniceException("Record reader not initialized");
    }

    this.isMapperOnly = props.getBoolean(VENICE_MAP_ONLY);
    if (isMapperOnly) {
      this.reducer = new VeniceReducer(false);
      reducer.configure(job);
    }
  }
}