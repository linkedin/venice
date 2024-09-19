package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.FilterChain;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.ttl.VeniceKafkaInputTTLFilter;
import com.linkedin.venice.hadoop.input.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.hadoop.mapreduce.datawriter.map.AbstractVeniceMapper;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;


/**
 * This class is designed specifically for {@link KafkaInputFormat}, and right now, it is doing simple pass-through.
 */
public class VeniceKafkaInputMapper extends AbstractVeniceMapper<KafkaInputMapperKey, KafkaInputMapperValue> {
  private static final RecordSerializer KAFKA_INPUT_MAPPER_KEY_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperKey.SCHEMA$);
  private static final RecordSerializer KAFKA_INPUT_MAPPER_VALUE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperValue.SCHEMA$);
  private FilterChain<KafkaInputMapperValue> veniceFilterChain;

  @Override
  protected AbstractVeniceRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> getRecordReader(
      VeniceProperties props) {
    throw new UnsupportedOperationException();
  }

  protected FilterChain<KafkaInputMapperValue> getFilterChain(final VeniceProperties props) {
    FilterChain<KafkaInputMapperValue> filterChain = null;
    boolean ttlEnabled = props.getBoolean(REPUSH_TTL_ENABLE, false);
    if (ttlEnabled) {
      try {
        filterChain = new FilterChain<>();
        filterChain.add(new VeniceKafkaInputTTLFilter(props));
      } catch (IOException e) {
        throw new VeniceException("failed to instantiate the ttl filter for KIF", e);
      }
    }
    return filterChain;
  }

  @Override
  protected void configureTask(VeniceProperties props) {
    /**
     * Do nothing but create the filter for {@link KafkaInputFormat}.
     */
    this.veniceFilterChain = getFilterChain(props);
  }

  @Override
  protected boolean process(
      KafkaInputMapperKey inputKey,
      KafkaInputMapperValue inputValue,
      AtomicReference<byte[]> keyRef,
      AtomicReference<byte[]> valueRef,
      DataWriterTaskTracker dataWriterTaskTracker) {
    if (veniceFilterChain != null && veniceFilterChain.apply(inputValue)) {
      dataWriterTaskTracker.trackRepushTtlFilteredRecord();
      return false;
    }
    byte[] serializedKey = KAFKA_INPUT_MAPPER_KEY_SERIALIZER.serialize(inputKey);
    keyRef.set(serializedKey);
    byte[] serializedValue = KAFKA_INPUT_MAPPER_VALUE_SERIALIZER.serialize(inputValue);
    valueRef.set(serializedValue);
    return true;
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(veniceFilterChain);
    super.close();
  }
}
