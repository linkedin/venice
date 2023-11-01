package com.linkedin.venice.hadoop.recordreader;

import com.linkedin.venice.hadoop.mapreduce.datawriter.reduce.VeniceReducer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.Pair;
import java.io.Closeable;
import java.util.Iterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An abstraction for a record reader that reads records from the configured input into Avro-serialized keys and values.
 * @param <INPUT_KEY> The format of the key as controlled by the input format
 * @param <INPUT_VALUE> The format of the value as controlled by the input format
 */
public abstract class AbstractVeniceRecordReader<INPUT_KEY, INPUT_VALUE> implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(AbstractVeniceRecordReader.class);

  protected String topicName;

  private String keySchemaStr;

  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;

  Object avroKey, avroValue;

  public AbstractVeniceRecordReader(String topicName) {
    this.topicName = topicName;
  }

  /**
   * Configure the record serializers
   */
  public void configure(String keySchemaStr, String valueSchemaStr) {
    this.keySchemaStr = keySchemaStr;
    keySerializer = new VeniceAvroKafkaSerializer(keySchemaStr);
    valueSerializer = new VeniceAvroKafkaSerializer(valueSchemaStr);
  }

  /**
   * Return an Avro output key
   */
  public abstract Object getAvroKey(INPUT_KEY inputKey, INPUT_VALUE inputValue);

  /**
   * return an Avro output value
   */
  public abstract Object getAvroValue(INPUT_KEY inputKey, INPUT_VALUE inputValue);

  /**
   * Return a serialized output key
   */
  public byte[] getKeyBytes(INPUT_KEY inputKey, INPUT_VALUE inputValue) {
    avroKey = getAvroKey(inputKey, inputValue);

    if (avroKey == null) {
      return null;
    }

    return keySerializer.serialize(topicName, avroKey);
  }

  /**
   * Return a serialized output value
   */
  public byte[] getValueBytes(INPUT_KEY inputKey, INPUT_VALUE inputValue) {
    avroValue = getAvroValue(inputKey, inputValue);

    if (avroValue == null) {
      return null;
    }

    return valueSerializer.serialize(topicName, avroValue);
  }

  /**
   * Return an Avro key schema string that will be used to init key serializer
   */
  public String getKeySchemaStr() {
    return keySchemaStr;
  }

  public abstract Iterator<Pair<byte[], byte[]>> iterator();

  /**
   * This is a helper that will be called in {@link VeniceReducer} to deserialize binary key
   */
  public VeniceKafkaSerializer getKeySerializer() {
    if (keySerializer == null) {
      LOGGER.warn("key serializer has not been initialized yet. Please call configure().");
    }

    return keySerializer;
  }

  /**
   * This is a helper that will be called in {@link VeniceReducer} to deserialize binary key
   */
  public VeniceKafkaSerializer getValueSerializer() {
    if (valueSerializer == null) {
      LOGGER.warn("value serializer has not been initialized yet. Please call configure().");
    }

    return valueSerializer;
  }
}
