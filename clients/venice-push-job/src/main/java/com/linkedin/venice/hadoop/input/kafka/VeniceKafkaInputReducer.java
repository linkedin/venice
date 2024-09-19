package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.FilterChain;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkAssembler;
import com.linkedin.venice.hadoop.input.kafka.ttl.VeniceChunkedPayloadTTLFilter;
import com.linkedin.venice.hadoop.mapreduce.datawriter.reduce.VeniceReducer;
import com.linkedin.venice.hadoop.task.datawriter.AbstractPartitionWriter;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is designed specifically for {@link KafkaInputFormat}, and right now, it will pick up the latest
 * entry according to the associated offset, and produce it to Kafka.
 */
public class VeniceKafkaInputReducer extends VeniceReducer {
  private static final Logger LOGGER = LogManager.getLogger(VeniceKafkaInputReducer.class);
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

  private CompressorFactory compressorFactory;
  private VeniceCompressor sourceVersionCompressor;
  private VeniceCompressor destVersionCompressor;
  private boolean passThrough = false;

  @Override
  protected void configureTask(VeniceProperties props) {
    super.configureTask(props);
    this.veniceFilterChain = initFilterChain(props);

    compressorFactory = new CompressorFactory();
    sourceVersionCompressor = KafkaInputUtils.getCompressor(
        compressorFactory,
        CompressionStrategy.valueOf(props.getString(KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY)),
        props.getString(KAFKA_INPUT_BROKER_URL),
        props.getString(KAFKA_INPUT_TOPIC),
        props);
    destVersionCompressor = KafkaInputUtils.getCompressor(
        compressorFactory,
        CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY)),
        props.getString(KAFKA_BOOTSTRAP_SERVERS),
        props.getString(TOPIC_PROP),
        props);
    passThrough = sourceVersionCompressor.equals(destVersionCompressor);
    if (passThrough) {
      LOGGER.info(
          "{} will do pass-through since both source version and"
              + " dest version are using the same compressor with compression strategy: {}",
          this.getClass().getSimpleName(),
          sourceVersionCompressor.getClass().getSimpleName());
    }
  }

  // For testing only
  protected void setSourceVersionCompressor(VeniceCompressor compressor) {
    this.sourceVersionCompressor = compressor;
    this.passThrough = this.sourceVersionCompressor.equals(destVersionCompressor);
  }

  // For testing only
  protected void setDestVersionCompressor(VeniceCompressor compressor) {
    this.destVersionCompressor = compressor;
    this.passThrough = this.destVersionCompressor.equals(sourceVersionCompressor);
  }

  protected byte[] compress(byte[] valueBytesFromSourceVersion) {
    if (valueBytesFromSourceVersion == null || passThrough) {
      return valueBytesFromSourceVersion;
    }
    try {
      // Decompress and then re-compress
      ByteBuffer decompressedValue =
          sourceVersionCompressor.decompress(valueBytesFromSourceVersion, 0, valueBytesFromSourceVersion.length);
      ByteBuffer reCompressedValue = destVersionCompressor.compress(decompressedValue, 0);
      return ByteUtils.extractByteArray(reCompressedValue);
    } catch (IOException e) {
      throw new VeniceException("Failed to re-compress object", e);
    }
  }

  /**
   * No need to print out duplicate keys since duplicate keys are expected in Kafka topics.
   */
  @Override
  protected DuplicateKeyPrinter initDuplicateKeyPrinter(VeniceProperties properties) {
    return null;
  }

  @Override
  protected AbstractPartitionWriter.VeniceWriterMessage extract(
      byte[] key,
      Iterator<byte[]> valueIterator,
      DataWriterTaskTracker dataWriterTaskTracker) {
    KafkaInputMapperKey mapperKey = KAFKA_INPUT_MAPPER_KEY_AVRO_SPECIFIC_DESERIALIZER.deserialize(key);
    byte[] keyBytes = ByteUtils.extractByteArray(mapperKey.key);
    if (!valueIterator.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " + ByteUtils.toHexString(keyBytes));
    }

    return extractor.extract(keyBytes, valueIterator, dataWriterTaskTracker);
  }

  @Override
  protected void setChunkingEnabled(boolean isChunkingEnabled) {
    super.setChunkingEnabled(isChunkingEnabled);
    if (isChunkingEnabled) {
      this.extractor = this::extractChunkedMessage;
      this.chunkAssembler = new ChunkAssembler(isRmdChunkingEnabled());
    } else {
      this.extractor = this::extractNonChunkedMessage;
      this.chunkAssembler = null;
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    Utils.closeQuietlyWithErrorLogged(veniceFilterChain);
    Utils.closeQuietlyWithErrorLogged(compressorFactory);
  }

  private interface MessageExtractor {
    AbstractPartitionWriter.VeniceWriterMessage extract(
        byte[] keyBytes,
        Iterator<byte[]> valueIterator,
        DataWriterTaskTracker dataWriterTaskTracker);
  }

  private AbstractPartitionWriter.VeniceWriterMessage extractChunkedMessage(
      final byte[] keyBytes,
      @Nonnull Iterator<byte[]> valueIterator,
      DataWriterTaskTracker dataWriterTaskTracker) {
    ChunkAssembler.ValueBytesAndSchemaId value = chunkAssembler.assembleAndGetValue(keyBytes, valueIterator);
    if (value == null) {
      return null;
    } else if (veniceFilterChain != null && veniceFilterChain.apply(value)) {
      dataWriterTaskTracker.trackRepushTtlFilteredRecord();
      return null;
    } else {
      if (value.getReplicationMetadataPayload().remaining() == 0) {
        return new AbstractPartitionWriter.VeniceWriterMessage(
            keyBytes,
            compress(value.getBytes()),
            value.getSchemaID(),
            getCallback(),
            isEnableWriteCompute(),
            getDerivedValueSchemaId());
      }
      return new AbstractPartitionWriter.VeniceWriterMessage(
          keyBytes,
          compress(value.getBytes()),
          value.getSchemaID(),
          value.getReplicationMetadataVersionId(),
          value.getReplicationMetadataPayload(),
          getCallback(),
          isEnableWriteCompute(),
          getDerivedValueSchemaId());
    }
  }

  private AbstractPartitionWriter.VeniceWriterMessage extractNonChunkedMessage(
      final byte[] keyBytes,
      @Nonnull Iterator<byte[]> valueIterator,
      DataWriterTaskTracker dataWriterTaskTracker) {
    if (!valueIterator.hasNext()) {
      throw new IllegalArgumentException("The valueIterator did not contain any value!");
    }

    byte[] latestValue = valueIterator.next();
    KafkaInputMapperValue latestMapperValue = KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER
        .deserialize(OPTIMIZED_BINARY_DECODER_FACTORY.createOptimizedBinaryDecoder(latestValue, 0, latestValue.length));
    if (latestMapperValue.valueType.equals(MapperValueType.DELETE)) {
      // Deleted record
      if (latestMapperValue.replicationMetadataPayload.remaining() != 0) {
        return new AbstractPartitionWriter.VeniceWriterMessage(
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
      return new AbstractPartitionWriter.VeniceWriterMessage(
          keyBytes,
          compress(valueBytes),
          latestMapperValue.schemaId,
          latestMapperValue.replicationMetadataVersionId,
          latestMapperValue.replicationMetadataPayload,
          getCallback(),
          isEnableWriteCompute(),
          getDerivedValueSchemaId());
    }
    return new AbstractPartitionWriter.VeniceWriterMessage(
        keyBytes,
        compress(valueBytes),
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
    boolean ttlEnabled = props.getBoolean(REPUSH_TTL_ENABLE, false);
    if (isChunkingEnabled() && ttlEnabled) {
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
