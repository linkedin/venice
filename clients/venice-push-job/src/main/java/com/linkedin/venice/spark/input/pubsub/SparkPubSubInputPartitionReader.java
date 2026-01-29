package com.linkedin.venice.spark.input.pubsub;

import static com.linkedin.venice.utils.Utils.EMPTY_BYTE_BUFFER;

import com.linkedin.venice.chunking.ChunkKeyValueTransformer;
import com.linkedin.venice.chunking.RawKeyBytesAndChunkedKeySuffix;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator.PubSubInputRecord;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;


/**
 * A Spark SQL data source partition reader implementation for Venice PubSub messages.
 * <p>
 * This reader consumes messages from a specific partition of a PubSub topic between
 * specified start and end offsets, converting them into Spark's {@link InternalRow} format.
 * The reader provides functionality for:
 * <ul>
 *   <li>Reading messages from a specific topic partition</li>
 *   <li>Filtering control messages when configured</li>
 *   <li>Tracking consumption progress</li>
 * </ul>
 * <p>
 * This class is part of the Venice Spark connector enabling ETL and KIF functionality.
 */
public class SparkPubSubInputPartitionReader implements PartitionReader<InternalRow> {
  private static final Logger LOGGER = LogManager.getLogger(SparkPubSubInputPartitionReader.class);
  private final PubSubSplitIterator pubSubSplitIterator;
  private final PubSubTopicPartition topicPartition;
  private final String region;
  private final boolean isChunkingEnabled;
  private final ChunkKeyValueTransformer keyTransformer;

  private GenericInternalRow currentRow;

  public SparkPubSubInputPartitionReader(
      SparkPubSubInputPartition inputPartition,
      PubSubConsumerAdapter consumer,
      String region,
      boolean useLogicalIndexOffset,
      boolean isChunkingEnabled,
      ChunkKeyValueTransformer keyTransformer) {
    this(
        inputPartition,
        region,
        new PubSubSplitIterator(consumer, inputPartition.getPubSubPartitionSplit(), useLogicalIndexOffset),
        isChunkingEnabled,
        keyTransformer);
  }

  /**
   * This is the common constructor that all other constructors delegate to.
   *
   * @param inputPartition the input partition to read from
   * @param region the region name
   * @param mockIterator the mock iterator for testing
   * @param isChunkingEnabled whether chunking is enabled (pass false for non-chunking tests)
   * @param keyTransformer the key transformer for chunking (pass null for non-chunking tests)
   */
  SparkPubSubInputPartitionReader(
      SparkPubSubInputPartition inputPartition,
      String region,
      PubSubSplitIterator mockIterator,
      boolean isChunkingEnabled,
      ChunkKeyValueTransformer keyTransformer) {
    this.topicPartition = inputPartition.getPubSubPartitionSplit().getPubSubTopicPartition();
    this.region = region;
    this.isChunkingEnabled = isChunkingEnabled;
    this.keyTransformer = keyTransformer;
    this.pubSubSplitIterator = mockIterator;
  }

  @Override
  public boolean next() throws IOException {
    // Pull the next decoded unit from the iterator. Null => no more data.
    PubSubInputRecord rec = pubSubSplitIterator.next();
    if (rec == null) {
      currentRow = null;
      return false;
    }

    DefaultPubSubMessage pubSubMessage = rec.getPubSubMessage();
    KafkaKey pubSubMessageKey = pubSubMessage.getKey();
    KafkaMessageEnvelope pubSubMessageValue = pubSubMessage.getValue();
    MessageType pubSubMessageType = MessageType.valueOf(pubSubMessageValue);

    // Spark row setup
    ByteBuffer value;
    int messageType;
    int schemaId;
    ByteBuffer replicationMetadataPayload;
    int replicationMetadataVersionId;

    switch (pubSubMessageType) {
      case PUT:
        Put put = (Put) pubSubMessageValue.getPayloadUnion();
        messageType = MessageType.PUT.getValue();
        value = put.getPutValue();
        schemaId = put.getSchemaId(); // chunking will be handled down the road in spark job.
        replicationMetadataPayload = put.getReplicationMetadataPayload();
        replicationMetadataVersionId = put.getReplicationMetadataVersionId();
        break;
      case DELETE:
        messageType = MessageType.DELETE.getValue();
        Delete delete = (Delete) pubSubMessageValue.getPayloadUnion();
        schemaId = delete.getSchemaId();
        value = EMPTY_BYTE_BUFFER;

        replicationMetadataPayload = delete.getReplicationMetadataPayload();
        replicationMetadataVersionId = delete.getReplicationMetadataVersionId();
        break;
      default:
        throw new IOException(
            "Unexpected message type: " + pubSubMessageType + " in " + topicPartition + " at "
                + pubSubMessage.getPosition());
    }

    // Key handling: if chunking is enabled, split the composite key to extract the raw user key
    // All chunks of the same value share the same user key but have different chunked suffixes.
    ByteBuffer key;
    ByteBuffer chunkedKeySuffix = null;
    if (isChunkingEnabled && keyTransformer != null) {
      ChunkKeyValueTransformer.KeyType keyType = ChunkKeyValueTransformer.getKeyType(pubSubMessageType, schemaId);
      RawKeyBytesAndChunkedKeySuffix parts = keyTransformer.splitChunkedKey(pubSubMessageKey.getKey(), keyType);
      key = parts.getRawKeyBytes(); // Extract only the user key (suffix removed)
      chunkedKeySuffix = parts.getChunkedKeySuffixBytes(); // Extract the suffix for chunk matching
    } else {
      // Non-chunked or chunking disabled: use the full key as-is
      key = ByteBuffer.wrap(pubSubMessageKey.getKey(), 0, pubSubMessageKey.getKeyLength());
    }

    /**
     *  See {@link com.linkedin.venice.spark.SparkConstants#RAW_PUBSUB_INPUT_TABLE_SCHEMA} for the schema definition.
     *  Enforce the region to be UTF8String for Spark compatibility and additionally handle ordering of columns per
     *  the schema.
     *
     *  Schema: region, partition, offset, message_type, schema_id, key, value, rmd_version_id, rmd_payload, chunked_key_suffix
     */
    currentRow = new GenericInternalRow(
        new Object[] { UTF8String.fromString(region), topicPartition.getPartitionNumber(), rec.getOffset(), messageType,
            schemaId, ByteUtils.extractByteArray(key), ByteUtils.extractByteArray(value), replicationMetadataVersionId,
            ByteUtils.extractByteArray(replicationMetadataPayload),
            chunkedKeySuffix != null ? ByteUtils.extractByteArray(chunkedKeySuffix) : null });

    logProgressPercent();
    return true;
  }

  @Override
  public InternalRow get() {
    // should return the same row if called multiple times
    return currentRow;
  }

  public float logProgressPercent() {
    return pubSubSplitIterator.getProgress() * 100.0f; // Convert to percentage
  }

  @Override
  public void close() throws IOException {
    try {
      pubSubSplitIterator.close();
    } catch (Exception e) {
      LOGGER.error("Error closing PubSubSplitIterator for topic partition: {}", topicPartition, e);
      throw new IOException("Failed to close PubSubSplitIterator", e);
    }
  }
}
