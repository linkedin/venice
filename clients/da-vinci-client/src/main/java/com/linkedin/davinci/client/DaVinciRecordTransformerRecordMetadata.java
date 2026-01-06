package com.linkedin.davinci.client;

import com.linkedin.venice.pubsub.api.PubSubPosition;


/**
 * Per record metadata wrapper for {@link DaVinciRecordTransformer}.
 */
public class DaVinciRecordTransformerRecordMetadata {
  public final static int SENTINEL_WRITER_SCHEMA_ID = -1;
  private final int writerSchemaId;
  private final long timestamp;
  private final PubSubPosition pubSubPosition;
  private final int payloadSize;
  private final java.nio.ByteBuffer replicationMetadataPayload;
  private final int replicationMetadataVersionId;

  public DaVinciRecordTransformerRecordMetadata(long timestamp, PubSubPosition pubSubPosition, int payloadSize) {
    this(SENTINEL_WRITER_SCHEMA_ID, timestamp, pubSubPosition, payloadSize, null, SENTINEL_WRITER_SCHEMA_ID);
  }

  public DaVinciRecordTransformerRecordMetadata(
      int writerSchemaId,
      long timestamp,
      PubSubPosition pubSubPosition,
      int payloadSize,
      java.nio.ByteBuffer replicationMetadataPayload,
      int replicationMetadataVersionId) {
    this.writerSchemaId = writerSchemaId;
    this.timestamp = timestamp;
    this.pubSubPosition = pubSubPosition;
    this.payloadSize = payloadSize;
    this.replicationMetadataPayload = replicationMetadataPayload;
    this.replicationMetadataVersionId = replicationMetadataVersionId;
  }

  /**
   * @return the schema ID that the record was written with if it's a PUT.
   * If the record came from disk, or it's a DELETE, it will be {@link #SENTINEL_WRITER_SCHEMA_ID}.
   */
  public int getWriterSchemaId() {
    return writerSchemaId;
  }

  /**
   * @return timestamp that the record was added to the version topic
   * If the record came from disk, it will be 0.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return the position of the record in the topic.
   * If the record came from disk, it will be the earliest position in the topic partition.
   */
  public PubSubPosition getPubSubPosition() {
    return pubSubPosition;
  }

  /**
   * @return the size in bytes of the key + value
   */
  public int getPayloadSize() {
    return payloadSize;
  }

  /**
   * @return the serialized replication metadata payload if it's a PUT.
   * If the record came from disk or it's a DELETE, it will be null.
   */
  public java.nio.ByteBuffer getReplicationMetadataPayload() {
    return replicationMetadataPayload;
  }

  /**
   * @return the corresponding replication metadata schema version id used to serialize the payload. If the payload is
   * null the corresponding id will be -1.
   */
  public int getReplicationMetadataVersionId() {
    return replicationMetadataVersionId;
  }
}
