package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.mapreduce.datawriter.partition.VeniceMRPartitioner;
import com.linkedin.venice.hadoop.mapreduce.engine.MapReduceEngineTaskConfigProvider;
import com.linkedin.venice.hadoop.task.datawriter.AbstractDataWriterTask;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.VenicePushJobConstants;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


/**
 * This class is a Combiner, which is a functionality of the MR framework where we can plug a {@link Reducer}
 * implementation to be executed within the {@link Mapper} task, on its output. This allows the Reducer to have less
 * work to do since part of it was already done in the Mappers. In the case of the {@link KafkaInputFormat}, we cannot
 * do all the same work that the {@link VeniceKafkaInputReducer} is doing, since that includes producing to Kafka, but
 * the part which is relevant to shift to Mappers is the compaction. We have observed that when the input partitions are
 * very large, then Reducers can run out of memory. By shifting the work to Mappers, we should be able to remove this
 * bottleneck and scale better, especially if used in combination with a low value for:
 * {@link VenicePushJobConstants#KAFKA_INPUT_MAX_RECORDS_PER_MAPPER}
 */
public class KafkaInputFormatCombiner extends AbstractDataWriterTask
    implements Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable>, JobConfigurable {
  private static final RecordDeserializer<KafkaInputMapperValue> KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(KafkaInputMapperValue.SCHEMA$, KafkaInputMapperValue.class);

  @Override
  public void reduce(
      BytesWritable key,
      Iterator<BytesWritable> values,
      OutputCollector<BytesWritable, BytesWritable> output,
      Reporter reporter) throws IOException {
    /**
     * N.B.: This class currently does nothing chunked topics, which is functional, but provides no
     *       scalability benefits. In theory, it is possible to extend the scalability benefits to
     *       chunked topics as well, but the logic is trickier, since it is not guaranteed that a
     *       given input split (i.e. based on the {@link VenicePushJob#KAFKA_INPUT_MAX_RECORDS_PER_MAPPER}
     *       config), would contain all chunks associated with a write. Therefore, some writes which
     *       are only partially present would still need to be passed through. Furthermore, the
     *       writes with lower offsets to the same key could not necessarily be compacted away
     *       unless we are guaranteed that a fully complete write exists for that key (because the
     *       partial write we let through may actually be partially failed, but we couldn't tell
     *       that from just one input split).
     */
    if (isChunkingEnabled() || key.getLength() == VeniceMRPartitioner.EMPTY_KEY_LENGTH) {
      while (values.hasNext()) {
        output.collect(key, values.next());
      }
      return;
    }

    if (!values.hasNext()) {
      /**
       * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
       */
      final byte[] keyBytes = key.copyBytes();
      throw new VeniceException("There is no value corresponding to key bytes: " + ByteUtils.toHexString(keyBytes));
    }

    Optional<BytesWritable> bytesWritableOptional = extractValueWithHighestOffset(values);
    if (bytesWritableOptional.isPresent()) {
      output.collect(key, bytesWritableOptional.get());
    }
  }

  @Override
  public void close() {
  }

  @Override
  protected void configureTask(VeniceProperties props) {
  }

  @Override
  public void configure(JobConf job) {
    super.configure(new MapReduceEngineTaskConfigProvider(job));
  }

  /**
   * @return the value with the largest offset for the purpose of compaction
   */
  private Optional<BytesWritable> extractValueWithHighestOffset(Iterator<BytesWritable> valueIterator) {
    if (!valueIterator.hasNext()) {
      throw new IllegalArgumentException("The valueIterator did not contain any value!");
    }
    // Just check the first entry because of secondary sorting.
    BytesWritable firstBytesWritable = valueIterator.next();
    KafkaInputMapperValue mapperValue = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(
        OptimizedBinaryDecoderFactory.defaultFactory()
            .createOptimizedBinaryDecoder(firstBytesWritable.getBytes(), 0, firstBytesWritable.getLength()));
    if (mapperValue.valueType.equals(MapperValueType.DELETE)) {
      return Optional.empty();
    }
    return Optional.of(firstBytesWritable);
  }
}
