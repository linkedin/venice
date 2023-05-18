package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.AbstractVeniceMapper;
import com.linkedin.venice.hadoop.AbstractVeniceRecordReader;
import com.linkedin.venice.hadoop.FilterChain;
import com.linkedin.venice.hadoop.MRJobCounterHelper;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.ttl.VeniceKafkaInputTTLFilter;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;


/**
 * This class is designed specifically for {@link KafkaInputFormat}, and right now, it is doing simple pass-through.
 */
public class VeniceKafkaInputMapper extends AbstractVeniceMapper<KafkaInputMapperKey, KafkaInputMapperValue> {
  private static final RecordSerializer KAFKA_INPUT_MAPPER_KEY_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperKey.SCHEMA$);
  private static final RecordSerializer KAFKA_INPUT_MAPPER_VALUE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperValue.SCHEMA$);

  @Override
  protected AbstractVeniceRecordReader<KafkaInputMapperKey, KafkaInputMapperValue> getRecordReader(
      VeniceProperties props) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected FilterChain<KafkaInputMapperValue> getFilterChain(final VeniceProperties props) {
    FilterChain<KafkaInputMapperValue> filterChain = null;
    long ttlInSeconds = props.getLong(VenicePushJob.REPUSH_TTL_IN_SECONDS, VenicePushJob.NOT_SET);
    if (ttlInSeconds != VenicePushJob.NOT_SET) {
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
  protected void configureTask(VeniceProperties props, JobConf job) {
    /**
     * Do nothing but create the filter for {@link KafkaInputFormat}.
     */
    this.veniceFilterChain = getFilterChain(props);
  }

  @Override
  protected boolean process(
      KafkaInputMapperKey inputKey,
      KafkaInputMapperValue inputValue,
      BytesWritable keyBW,
      BytesWritable valueBW,
      Reporter reporter) {
    if (veniceFilterChain != null && veniceFilterChain.apply(inputValue)) {
      MRJobCounterHelper.incrRepushTtlFilterCount(reporter, 1L);
      return false;
    }
    byte[] serializedKey = KAFKA_INPUT_MAPPER_KEY_SERIALIZER.serialize(inputKey);
    keyBW.set(serializedKey, 0, serializedKey.length);
    byte[] serializedValue = KAFKA_INPUT_MAPPER_VALUE_SERIALIZER.serialize(inputValue);
    valueBW.set(serializedValue, 0, serializedValue.length);
    return true;
  }
}
