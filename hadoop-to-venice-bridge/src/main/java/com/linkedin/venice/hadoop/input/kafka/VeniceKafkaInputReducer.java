package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VeniceReducer;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkAssembler;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.ByteUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;
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
  private ChunkAssembler chunkAssembler;

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
  protected Optional<VeniceWriterMessage> extract(
      BytesWritable key,
      Iterator<BytesWritable> valueIterator,
      Reporter reporter) {
    /**
     * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
     */
    final byte[] keyBytes = key.copyBytes();
    if (!valueIterator.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " + ByteUtils.toHexString(keyBytes));
    }
    List<KafkaInputMapperValue> mapperValues = getMapperValues(valueIterator);
    return isChunkingEnabled()
        ? extractChunkedMessage(keyBytes, mapperValues)
        : extractNonChunkedMessage(keyBytes, mapperValues);
  }

  private Optional<VeniceWriterMessage> extractChunkedMessage(
      final byte[] keyBytes,
      @Nonnull List<KafkaInputMapperValue> mapperValues) {
    Validate.notEmpty(mapperValues);
    return getChunkAssembler().assembleAndGetValue(keyBytes, mapperValues)
        .map(
            valueBytesAndSchemaId -> new VeniceWriterMessage(
                keyBytes,
                valueBytesAndSchemaId.getBytes(),
                valueBytesAndSchemaId.getSchemaID(),
                getCallback(),
                isEnableWriteCompute(),
                getDerivedValueSchemaId()));
  }

  private ChunkAssembler getChunkAssembler() {
    if (chunkAssembler == null) {
      chunkAssembler = new ChunkAssembler();
    }
    return chunkAssembler;
  }

  private Optional<VeniceWriterMessage> extractNonChunkedMessage(
      final byte[] keyBytes,
      @Nonnull List<KafkaInputMapperValue> mapperValues) {
    Validate.notEmpty(mapperValues);
    // Only get the value with the largest offset for the purpose of compaction
    KafkaInputMapperValue lastValue = null;
    long largestOffset = Long.MIN_VALUE;
    for (KafkaInputMapperValue mapperValue: mapperValues) {
      if (mapperValue.offset > largestOffset) {
        lastValue = mapperValue;
        largestOffset = mapperValue.offset;
      }
    }
    if (lastValue.valueType.equals(MapperValueType.DELETE)) {
      // Deleted record
      if (lastValue.replicationMetadataPayload.remaining() != 0) {
        return Optional.of(
            new VeniceWriterMessage(
                keyBytes,
                null,
                lastValue.schemaId,
                lastValue.replicationMetadataVersionId,
                lastValue.replicationMetadataPayload,
                getCallback(),
                isEnableWriteCompute(),
                getDerivedValueSchemaId()));
      }
      return Optional.empty();
    }
    byte[] valueBytes = ByteUtils.extractByteArray(lastValue.value);
    if (lastValue.replicationMetadataPayload.remaining() != 0) {
      return Optional.of(
          new VeniceWriterMessage(
              keyBytes,
              valueBytes,
              lastValue.schemaId,
              lastValue.replicationMetadataVersionId,
              lastValue.replicationMetadataPayload,
              getCallback(),
              isEnableWriteCompute(),
              getDerivedValueSchemaId()));
    }
    return Optional.of(
        new VeniceWriterMessage(
            keyBytes,
            valueBytes,
            lastValue.schemaId,
            getCallback(),
            isEnableWriteCompute(),
            getDerivedValueSchemaId()));
  }

  private List<KafkaInputMapperValue> getMapperValues(Iterator<BytesWritable> valueIterator) {
    List<KafkaInputMapperValue> mapperValues = new ArrayList<>();
    while (valueIterator.hasNext()) {
      mapperValues
          .add(KAFKA_INPUT_MAPPER_VALUE_AVRO_SPECIFIC_DESERIALIZER.deserialize(valueIterator.next().copyBytes()));
    }
    return mapperValues;
  }
}
