package com.linkedin.venice.schema.rmd.v1;

/**
 * A POJO containing an index and a timestamp for a collection element. Note that this timestamp could be any timestamp.
 * In other words, it does not have to be just active timestamp.
 */
public class ElementTimestampAndIdx {
  private final long timestamp;
  private final int idx;

  ElementTimestampAndIdx(long timestamp, int idx) {
    if (idx < 0) {
      throw new IllegalArgumentException("Index cannot be negative. Got: " + idx);
    }
    this.idx = idx;
    this.timestamp = timestamp;
  }

  public int getIdx() {
    return idx;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
