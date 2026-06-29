package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import java.nio.ByteBuffer;


/**
 * Decodes a consumed real-time (RT) {@link DefaultPubSubMessage} into a normalized {@link SnapshotAtTRtRecord} for
 * the snapshot-at-T merge, tagging it with the source region's {@code coloId}. Control / Global-RT-DIV messages and
 * records whose write timestamp exceeds the cutoff are dropped (returns {@code null}).
 *
 * <p>This is shared by the single-process {@link SnapshotAtTRtReader} and the distributed reader
 * ({@link SnapshotAtTRtSplitReader}) so both produce byte-for-byte identical records from the same RT message.
 */
public final class SnapshotAtTRtRecordDecoder {
  private SnapshotAtTRtRecordDecoder() {
  }

  /**
   * @param message a consumed RT message
   * @param coloId the colo id of the region this message came from (for cross-region conflict resolution)
   * @param cutoffTimestampMs include only records with write timestamp &le; this; {@code <= 0} means no bound
   * @return the normalized record, or {@code null} if the message is a control / Global-RT-DIV message, is past the
   *         cutoff, or is not a PUT/UPDATE/DELETE
   */
  public static SnapshotAtTRtRecord decode(DefaultPubSubMessage message, int coloId, long cutoffTimestampMs) {
    KafkaKey key = message.getKey();
    if (key.isControlMessage() || key.isGlobalRtDiv()) {
      return null;
    }
    KafkaMessageEnvelope envelope = message.getValue();
    long writeTimestamp = envelope.producerMetadata.logicalTimestamp >= 0
        ? envelope.producerMetadata.logicalTimestamp
        : envelope.producerMetadata.messageTimestamp;
    if (cutoffTimestampMs > 0 && writeTimestamp > cutoffTimestampMs) {
      return null;
    }
    // Copy the key/value bytes out of the consumed message immediately: the message envelope's buffers may be
    // pooled/reused by the consumer on the next poll, and these records are processed only after the whole read
    // completes.
    ByteBuffer keyBytes = copy(ByteBuffer.wrap(key.getKey(), 0, key.getKeyLength()));
    MessageType messageType = MessageType.valueOf(envelope);
    switch (messageType) {
      case PUT:
        Put put = (Put) envelope.payloadUnion;
        return new SnapshotAtTRtRecord(
            SnapshotAtTRtRecord.Op.PUT,
            keyBytes,
            copy(put.putValue),
            put.schemaId,
            -1,
            writeTimestamp,
            coloId);
      case UPDATE:
        Update update = (Update) envelope.payloadUnion;
        return new SnapshotAtTRtRecord(
            SnapshotAtTRtRecord.Op.UPDATE,
            keyBytes,
            copy(update.updateValue),
            update.schemaId,
            update.updateSchemaId,
            writeTimestamp,
            coloId);
      case DELETE:
        Delete delete = (Delete) envelope.payloadUnion;
        return new SnapshotAtTRtRecord(
            SnapshotAtTRtRecord.Op.DELETE,
            keyBytes,
            null,
            delete.schemaId,
            -1,
            writeTimestamp,
            coloId);
      default:
        return null;
    }
  }

  /** Copy a buffer's remaining content into a fresh, independently-owned ByteBuffer. */
  static ByteBuffer copy(ByteBuffer source) {
    if (source == null) {
      return null;
    }
    ByteBuffer duplicate = source.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return ByteBuffer.wrap(bytes);
  }
}
