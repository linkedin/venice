package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.hadoop.AbstractVeniceFilter;
import com.linkedin.venice.hadoop.AbstractVeniceMapper;
import com.linkedin.venice.hadoop.AbstractVeniceRecordReader;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.ttl.VeniceKafkaInputTTLFilter;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;


/**
 * This class is designed specifically for {@link KafkaInputFormat}, and right now, it is doing simple pass-through.
 */
public class VeniceKafkaInputMapper extends AbstractVeniceMapper<BytesWritable, KafkaInputMapperValue> {
  private static final RecordSerializer KAFKA_INPUT_MAPPER_VALUE_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(KafkaInputMapperValue.SCHEMA$);

  @Override
  protected AbstractVeniceRecordReader<BytesWritable, KafkaInputMapperValue> getRecordReader(VeniceProperties props) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Optional<AbstractVeniceFilter<KafkaInputMapperValue>> getFilter(final VeniceProperties props) {
    long ttlInHours = props.getLong(VenicePushJob.REPUSH_TTL_IN_HOURS, VenicePushJob.NOT_SET);
    if (ttlInHours != VenicePushJob.NOT_SET) {
      VeniceKafkaInputTTLFilter filter = new VeniceKafkaInputTTLFilter(props);
      return Optional.of(filter);
    }
    return Optional.empty();
  }

  @Override
  protected void configureTask(VeniceProperties props, JobConf job) {
    /**
     * Do nothing for {@link KafkaInputFormat} for now, and if we need to support compression rebuild during re-push,
     * this function needs to be changed.
     */
    this.veniceFilter = getFilter(props);
  }

  @Override
  protected boolean process(
      BytesWritable inputKey,
      KafkaInputMapperValue inputValue,
      BytesWritable keyBW,
      BytesWritable valueBW,
      Reporter reporter) {
    if (veniceFilter.isPresent() && veniceFilter.get().applyRecursively(inputValue)) {
      return false;
    }
    keyBW.set(inputKey);
    byte[] serializedValue = KAFKA_INPUT_MAPPER_VALUE_SERIALIZER.serialize(inputValue, AvroSerializer.REUSE.get());
    valueBW.set(serializedValue, 0, serializedValue.length);
    return true;
  }
}
