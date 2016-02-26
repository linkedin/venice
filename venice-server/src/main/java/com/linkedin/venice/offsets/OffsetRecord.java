package com.linkedin.venice.offsets;

import com.linkedin.venice.utils.ByteUtils;


public class OffsetRecord {
    private final long offset;
    private final long eventTimeEpochMs;
    private final long processingTimeEpochMs;

    public static final OffsetRecord NON_EXISTENT_OFFSET = new OffsetRecord(0,0);

    private void validateOffSet(long offset) {
        if(offset < 0) {
            throw new IllegalArgumentException("Invalid OffSet " + offset);
        }
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
