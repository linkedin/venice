package com.linkedin.venice.kafka.consumer.offsets;

import com.linkedin.venice.utils.ByteUtils;


public class OffsetRecord {
  long offset;
  long recordedTimeMs;

  public OffsetRecord(long offset, long recordedTime) {
    this.offset = offset;
    this.recordedTimeMs = recordedTime;
  }

  /**
   *
   * @param bytes to deserialize from
   * @param offset starting offset in the bytes from which this object can be constructed
   *
   *TODO Get rid of second parameter offset if we knw 0 is always the starting index of the bytes to be deserialized.
   */
  public OffsetRecord(byte[] bytes, int offset) {
    if (bytes == null || bytes.length <= offset) {
      throw new IllegalArgumentException("Invalid byte array for serialization - no bytes to read");
    }
    this.offset = ByteUtils.readLong(bytes, offset);
    this.recordedTimeMs = ByteUtils.readLong(bytes, offset + ByteUtils.SIZE_OF_LONG);
  }

  public long getOffset() {
    return this.offset;
  }

  public long getRecordedTimeMs() {
    return this.recordedTimeMs;
  }

  /**
   * serialize to bytes
   *
   * @return byte[]
   */
  public byte[] toBytes() {
    byte[] res = new byte[ByteUtils.SIZE_OF_LONG * 2];
    ByteUtils.writeLong(res, this.offset, 0);
    ByteUtils.writeLong(res, this.recordedTimeMs, 0 + ByteUtils.SIZE_OF_LONG);
    return res;
  }
}
