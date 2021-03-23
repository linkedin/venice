package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VeniceReducer;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.ByteUtils;
import java.util.Iterator;
import java.util.Optional;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;


/**
 * This class is designed specifically for {@link KafkaInputFormat}, and right now, it will pick up the latest
 * entry according to the associated offset, and produce it to Kafka.
 */
public class VeniceKafkaInputReducer extends VeniceReducer {
  private static final RecordDeserializer<KafkaInputMapperValue> KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(KafkaInputMapperValue.SCHEMA$, KafkaInputMapperValue.class);

  /**
   * No need to print out duplicate keys since duplicate keys are expected in Kafka topics.
   * @param job
   * @return
   */
  @Override
  protected DuplicateKeyPrinter initDuplicateKeyPrinter(JobConf job) {
    return null;
  }

  @Override
  protected Optional<VeniceWriterMessage> extract(BytesWritable key, Iterator<BytesWritable> values, Reporter reporter) {
    /**
     * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
     */
    byte[] keyBytes = key.copyBytes();
    if (!values.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " +
          ByteUtils.toHexString(keyBytes));
    }
    /**
     * Iterate all the values and find out the one with the latest offset.
     */
    long lastOffset = -1;
    KafkaInputMapperValue finalValue = null;
    while (values.hasNext()) {
      KafkaInputMapperValue current = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(values.next().copyBytes());
      if (current.offset > lastOffset) {
        lastOffset = current.offset;
        finalValue = current;
      }
    }
    if (finalValue.valueType.equals(MapperValueType.DELETE)) {
      // Deleted record
      return Optional.empty();
    }
    byte[] valueBytes = ByteUtils.extractByteArray(finalValue.value);
    return Optional.of(new VeniceWriterMessage(keyBytes, valueBytes, finalValue.schemaId));
  }
}
