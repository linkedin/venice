package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroSerializer;
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

  protected String topicName;

  private boolean checkingRecordSize;

  private boolean isMapperOnly;
  private VeniceReducer reducer = null;

  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;

  @Override
  public void map(INPUT_KEY inputKey, INPUT_VALUE inputValue, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter)
  throws IOException {
    Object avroKey = getAvroKey(inputKey, inputValue);
    Object avroValue = getAvroValue(inputKey, inputValue);

    if (avroKey == null) {
      throw new VeniceException("Mapper received a empty key record");
    }

    if (avroValue == null) {
      LOGGER.warn("Received null record, skip.");
      reporter.incrCounter(COUNTER_GROUP_KAFKA, EMPTY_RECORD, 1);
      return;
    }

    byte[] recordKey = keySerializer.serialize(topicName, avroKey);
    byte[] recordValue = valueSerializer.serialize(topicName, avroValue);

    if (isMapperOnly) {
      reducer.sendMessageToKafka(recordKey, recordValue, reporter);
      return;
    }

    if (checkingRecordSize) {
      reporter.incrCounter(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_KEY_SIZE, recordKey.length);
      reporter.incrCounter(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_VALUE_SIZE, recordValue.length);
    }

    output.collect(new BytesWritable(recordKey), new BytesWritable(recordValue));
  }

  /**
   * Return an Avro output key
   */
  protected abstract Object getAvroKey(INPUT_KEY inputKey, INPUT_VALUE inputValue);

  /**
   * return an Avro output value
   */
  protected abstract Object getAvroValue(INPUT_KEY inputKey, INPUT_VALUE inputValue);

  /**
   * Return an Avro key schema string that will be used to init key serializer
   */
  abstract String getKeySchemaStr();

  /**
   * Return an Avro value schema string that will be used to init value serializer
   */
  abstract String getValueSchemaStr();

  /**
   * An optional method in case when child classes want to setup anything. It will be called before
   * {@link #configure(VeniceProperties)}
   */
  protected void configure(VeniceProperties props) {}

  @Override
  public void close() throws IOException {
    //no-op
  }

  /**
   * This is a helper that will be called in {@link VeniceReducer} to deserialize binary key
   */
  VeniceKafkaSerializer getKeySerializer() {
    if (keySerializer == null) {
      LOGGER.warn("key serializer has not been initialized yet. Please call configure().");
    }

    return keySerializer;
  }

  @Override
  public void configure(JobConf job) {
    VeniceProperties props = HadoopUtils.getVeniceProps(job);

    configure(props);

    this.topicName = props.getString(TOPIC_PROP);
    this.checkingRecordSize =
        props.getLong(STORAGE_QUOTA_PROP) != Store.UNLIMITED_STORAGE_QUOTA;

    this.isMapperOnly = props.getBoolean(VENICE_MAP_ONLY);
    if (isMapperOnly) {
      this.reducer = new VeniceReducer(false);
      reducer.configure(job);
    }

    keySerializer = new VeniceAvroSerializer(getKeySchemaStr());
    valueSerializer = new VeniceAvroSerializer(getValueSchemaStr());
  }
}