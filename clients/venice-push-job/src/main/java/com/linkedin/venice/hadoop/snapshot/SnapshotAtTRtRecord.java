package com.linkedin.venice.hadoop.snapshot;

import java.nio.ByteBuffer;


/**
 * One real-time (RT) record read from a region's RT topic, normalized for the offline snapshot-at-T merge.
 * Carries everything {@link SnapshotAtTRecordMerger} needs to fold it onto a batch base: the operation, the
 * key, the payload (full value for PUT, write-compute bytes for UPDATE), the value/update schema ids, the
 * write timestamp used for conflict resolution, and the source region's colo id.
 */
public class SnapshotAtTRtRecord {
  public enum Op {
    PUT, UPDATE, DELETE
  }

  private final Op op;
  private final ByteBuffer key;
  // Full value bytes for PUT, write-compute (partial-update) bytes for UPDATE, null for DELETE.
  private final ByteBuffer payload;
  private final int valueSchemaId;
  // Only meaningful for UPDATE (the write-compute / derived schema protocol version).
  private final int updateProtocolVersion;
  private final long writeTimestamp;
  private final int coloId;

  public SnapshotAtTRtRecord(
      Op op,
      ByteBuffer key,
      ByteBuffer payload,
      int valueSchemaId,
      int updateProtocolVersion,
      long writeTimestamp,
      int coloId) {
    this.op = op;
    this.key = key;
    this.payload = payload;
    this.valueSchemaId = valueSchemaId;
    this.updateProtocolVersion = updateProtocolVersion;
    this.writeTimestamp = writeTimestamp;
    this.coloId = coloId;
  }

  public Op getOp() {
    return op;
  }

  public ByteBuffer getKey() {
    return key;
  }

  public ByteBuffer getPayload() {
    return payload;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public int getUpdateProtocolVersion() {
    return updateProtocolVersion;
  }

  public long getWriteTimestamp() {
    return writeTimestamp;
  }

  public int getColoId() {
    return coloId;
  }
}
