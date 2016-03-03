package com.linkedin.venice.offsets;

import com.linkedin.venice.utils.ByteUtils;


public class OffsetRecord {
    private final long offset;
    private final long eventTimeEpochMs;
    private final long processingTimeEpochMs;

    // Offset 0 is still a valid offset, Using that will cause a message to be skipped.
    public static final long LOWEST_OFFSET = -1;
    public static final OffsetRecord NON_EXISTENT_OFFSET = new OffsetRecord(LOWEST_OFFSET,0);

    private void validateOffSet(long offset) {
        if(offset < LOWEST_OFFSET) {
            throw new IllegalArgumentException("Invalid OffSet " + offset);
        }
    }

    public OffsetRecord(long offset) {
        this(offset, System.currentTimeMillis());
    }

    public OffsetRecord(long offset, long recordedTime) {
        validateOffSet(offset);
        this.offset = offset;
        this.eventTimeEpochMs = recordedTime;
        this.processingTimeEpochMs = System.currentTimeMillis();
    }

    /**
     * @param bytes to deserialize from
     *
     * TODO Get rid of second parameter offset if we knw 0 is always the starting index of the bytes to be de serialized.
     */
    public OffsetRecord(byte[] bytes) {
        if (bytes == null || bytes.length < 3*ByteUtils.SIZE_OF_LONG) {
            throw new IllegalArgumentException("Invalid byte array for serialization - no bytes to read");
        }
        this.offset = ByteUtils.readLong(bytes, 0);
        this.eventTimeEpochMs = ByteUtils.readLong(bytes, ByteUtils.SIZE_OF_LONG);
        this.processingTimeEpochMs = ByteUtils.readLong(bytes, 2*ByteUtils.SIZE_OF_LONG);

        validateOffSet(offset);
    }

    public long getOffset() {
        return this.offset;
    }

    public long getEventTimeEpochMs() {
        return this.eventTimeEpochMs;
    }

    public long getProcessingTimeEpochMs() { return this.processingTimeEpochMs; }

    @Override
    public String toString() {
        return "OffsetRecord{" +
                "offset=" + offset +
                ", eventTimeEpochMs=" + eventTimeEpochMs +
                ", processingTimeEpochMs=" + processingTimeEpochMs +
                '}';
    }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OffsetRecord that = (OffsetRecord) o;

    return offset == that.offset;
  }

  @Override
  public int hashCode() {
    return (int) (offset ^ (offset >>> 32));
  }

  /**
     * serialize to bytes
     *
     * @return byte[]
     */
    public byte[] toBytes() {
        byte[] res = new byte[ByteUtils.SIZE_OF_LONG * 3];
        ByteUtils.writeLong(res, this.offset, 0);
        ByteUtils.writeLong(res, this.eventTimeEpochMs,  ByteUtils.SIZE_OF_LONG);
        ByteUtils.writeLong(res, this.processingTimeEpochMs, 2 * ByteUtils.SIZE_OF_LONG);
        return res;
    }
}
