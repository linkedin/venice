package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.FilterChain;
import com.linkedin.venice.hadoop.MRJobCounterHelper;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.VeniceReducer;
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
import java.util.Iterator;
import javax.annotation.Nonnull;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;


/**
 * This class is designed specifically for {@link KafkaInputFormat}, and right now, it will pick up the latest
 * entry according to the associated offset, and produce it to Kafka.
 */
public class VeniceKafkaInputReducer extends VeniceReducer {
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
    /**
     * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
     */
    final byte[] keyBytes = key.copyBytes();
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
    // Only get the value with the largest offset for the purpose of compaction
    KafkaInputMapperValue mapperValue = null;
    long largestOffset = Long.MIN_VALUE;
    byte[] mapperValueBytes;
    byte[] lastValueBytes = null;
    while (valueIterator.hasNext()) {
      mapperValueBytes = valueIterator.next().copyBytes();
      mapperValue = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(mapperValue, mapperValueBytes);
      if (mapperValue.offset > largestOffset) {
        lastValueBytes = mapperValueBytes;
        largestOffset = mapperValue.offset;
      }
    }
    if (lastValueBytes == null) {
      throw new IllegalStateException("lastValueBytes should not be null!");
    }
    KafkaInputMapperValue lastValue =
        KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(mapperValue, lastValueBytes);
    if (lastValue.valueType.equals(MapperValueType.DELETE)) {
      // Deleted record
      if (lastValue.replicationMetadataPayload.remaining() != 0) {
        return new VeniceWriterMessage(
            keyBytes,
            null,
            lastValue.schemaId,
            lastValue.replicationMetadataVersionId,
            lastValue.replicationMetadataPayload,
            getCallback(),
            isEnableWriteCompute(),
            getDerivedValueSchemaId());
      }
      return null;
    }
    byte[] valueBytes = ByteUtils.extractByteArray(lastValue.value);
    if (lastValue.replicationMetadataPayload.remaining() != 0) {
      return new VeniceWriterMessage(
          keyBytes,
          valueBytes,
          lastValue.schemaId,
          lastValue.replicationMetadataVersionId,
          lastValue.replicationMetadataPayload,
          getCallback(),
          isEnableWriteCompute(),
          getDerivedValueSchemaId());
    }
    return new VeniceWriterMessage(
        keyBytes,
        valueBytes,
        lastValue.schemaId,
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
