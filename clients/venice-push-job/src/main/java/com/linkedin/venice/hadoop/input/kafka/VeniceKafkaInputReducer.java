package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.FilterChain;
import com.linkedin.venice.hadoop.MRJobCounterHelper;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.VeniceReducer;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkAssembler;
import com.linkedin.venice.hadoop.input.kafka.ttl.VeniceChunkedPayloadTTLFilter;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.annotation.Nonnull;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;


/**
 * This class is designed specifically for {@link KafkaInputFormat}, and right now, it will pick up the latest
 * entry according to the associated offset, and produce it to Kafka.
 */
public class VeniceKafkaInputReducer extends VeniceReducer {
  private static final OptimizedBinaryDecoderFactory OPTIMIZED_BINARY_DECODER_FACTORY =
      OptimizedBinaryDecoderFactory.defaultFactory();
  private static final RecordDeserializer<KafkaInputMapperKey> KAFKA_INPUT_MAPPER_KEY_AVRO_SPECIFIC_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(KafkaInputMapperKey.SCHEMA$, KafkaInputMapperKey.class);
  private static final RecordDeserializer<KafkaInputMapperValue> KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(KafkaInputMapperValue.SCHEMA$, KafkaInputMapperValue.class);
  private ChunkAssembler chunkAssembler = null;
  private MessageExtractor extractor = this::extractNonChunkedMessage;

  protected FilterChain<ChunkAssembler.ValueBytesAndSchemaId> veniceFilterChain;

  @Override
  protected void configureTask(VeniceProperties props, JobConf job) {
    super.configureTask(props, job);
    this.veniceFilterChain = initFilterChain(props);
  }

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
  protected VeniceWriterMessage extract(BytesWritable key, Iterator<BytesWritable> valueIterator, Reporter reporter) {
    KafkaInputMapperKey mapperKey = KAFKA_INPUT_MAPPER_KEY_AVRO_SPECIFIC_DESERIALIZER
        .deserialize(ByteBuffer.wrap(key.getBytes(), 0, key.getLength()));
    byte[] keyBytes = mapperKey.key.array();
    if (!valueIterator.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " + ByteUtils.toHexString(keyBytes));
    }

    return extractor.extract(keyBytes, valueIterator, reporter);
  }

  @Override
  protected void setChunkingEnabled(boolean isChunkingEnabled) {
    super.setChunkingEnabled(isChunkingEnabled);
    if (isChunkingEnabled) {
      this.extractor = this::extractChunkedMessage;
      this.chunkAssembler = new ChunkAssembler();
    } else {
      this.extractor = this::extractNonChunkedMessage;
      this.chunkAssembler = null;
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    Utils.closeQuietlyWithErrorLogged(veniceFilterChain);
  }

  private interface MessageExtractor {
    VeniceWriterMessage extract(byte[] keyBytes, Iterator<BytesWritable> valueIterator, Reporter reporter);
  }

  private VeniceWriterMessage extractChunkedMessage(
      final byte[] keyBytes,
      @Nonnull Iterator<BytesWritable> valueIterator,
      Reporter reporter) {
    ChunkAssembler.ValueBytesAndSchemaId value = chunkAssembler.assembleAndGetValue(keyBytes, valueIterator);
    if (value == null) {
      return null;
    } else if (veniceFilterChain != null && veniceFilterChain.apply(value)) {
      MRJobCounterHelper.incrRepushTtlFilterCount(reporter, 1L);
      return null;
    } else {
      if (value.getReplicationMetadataPayload().remaining() == 0) {
        return new VeniceWriterMessage(
            keyBytes,
            value.getBytes(),
            value.getSchemaID(),
            getCallback(),
            isEnableWriteCompute(),
            getDerivedValueSchemaId());
      }
      return new VeniceWriterMessage(
          keyBytes,
          value.getBytes(),
          value.getSchemaID(),
          value.getReplicationMetadataVersionId(),
          value.getReplicationMetadataPayload(),
          getCallback(),
          isEnableWriteCompute(),
          getDerivedValueSchemaId());
    }
  }

  private VeniceWriterMessage extractNonChunkedMessage(
      final byte[] keyBytes,
      @Nonnull Iterator<BytesWritable> valueIterator,
      Reporter reporter) {
    if (!valueIterator.hasNext()) {
      throw new IllegalArgumentException("The valueIterator did not contain any value!");
    }

    BytesWritable latestValue = valueIterator.next();
    KafkaInputMapperValue latestMapperValue = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(
        OPTIMIZED_BINARY_DECODER_FACTORY
            .createOptimizedBinaryDecoder(latestValue.getBytes(), 0, latestValue.getLength()));
    if (latestMapperValue.valueType.equals(MapperValueType.DELETE)) {
      // Deleted record
      if (latestMapperValue.replicationMetadataPayload.remaining() != 0) {
        return new VeniceWriterMessage(
            keyBytes,
            null,
            latestMapperValue.schemaId,
            latestMapperValue.replicationMetadataVersionId,
            latestMapperValue.replicationMetadataPayload,
            getCallback(),
            isEnableWriteCompute(),
            getDerivedValueSchemaId());
      }
      return null;
    }
    byte[] valueBytes = ByteUtils.extractByteArray(latestMapperValue.value);
    if (latestMapperValue.replicationMetadataPayload.remaining() != 0) {
      return new VeniceWriterMessage(
          keyBytes,
          valueBytes,
          latestMapperValue.schemaId,
          latestMapperValue.replicationMetadataVersionId,
          latestMapperValue.replicationMetadataPayload,
          getCallback(),
          isEnableWriteCompute(),
          getDerivedValueSchemaId());
    }
    return new VeniceWriterMessage(
        keyBytes,
        valueBytes,
        latestMapperValue.schemaId,
        getCallback(),
        isEnableWriteCompute(),
        getDerivedValueSchemaId());
  }

  /**
   * Initialize filter chains in the reducer stage to support filtering when chunking is enabled.
   * @param props
   */
  FilterChain<ChunkAssembler.ValueBytesAndSchemaId> initFilterChain(VeniceProperties props) {
    FilterChain<ChunkAssembler.ValueBytesAndSchemaId> filterChain = null;
    long ttlInSeconds = props.getLong(VenicePushJob.REPUSH_TTL_IN_SECONDS, VenicePushJob.NOT_SET);
    if (isChunkingEnabled() && ttlInSeconds != VenicePushJob.NOT_SET) {
      try {
        filterChain = new FilterChain<>();
        filterChain.add(new VeniceChunkedPayloadTTLFilter(props));
      } catch (IOException e) {
        throw new VeniceException("failed to instantiate the ttl filter for chunked payload", e);
      }
    }
    return filterChain;
  }
}
