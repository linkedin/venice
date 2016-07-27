package com.linkedin.venice.writer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A segment is a sequence of messages sent by a single producer into a single partition.
 *
 * The same producer will maintain a different segment in each of the partitions it writes
 * into. On the other hand, many distinct producers can maintain their own segment for the
 * same partition, in which case, the messages contained in these various segments will be
 * interleaved.
 *
 * This class keeps track of the state of a segment:
 * - The partition it belongs to.
 * - Its segmentNumber number within its partition.
 * - Whether it has started.
 * - Whether it has ended.
 * - The current sequence number.
 *
 * N.B.: Visibility is package private on purpose, as this is an implementation detail of
 * the {@link VeniceWriter}. Please consider carefully if you think there is a need to use
 * this class from another context.
 */
class Segment {
  // Immutable state
  private final int partition;
  private final int segmentNumber;

  // Mutable state
  private boolean started = false;
  private boolean ended = false;
  private AtomicInteger sequenceNumber = new AtomicInteger(0);

  Segment(int partition, int segmentNumber) {
    this.partition = partition;
    this.segmentNumber = segmentNumber;
  }

  public int getSegmentNumber() {
    return segmentNumber;
  }

  public int getPartition() {
    return partition;
  }

  public int getAndIncrementSequenceNumber() {
    return sequenceNumber.getAndIncrement();
  }

  public boolean isStarted() {
    return started;
  }

  public boolean isEnded() {
    return ended;
  }

  public void start() {
    started = true;
  }

  public void end() {
    ended = true;
  }

  @Override
  public int hashCode() {
    int hash = 17;
    hash = hash * 31 + partition;
    hash = hash * 31 + segmentNumber;
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Segment) {
      Segment otherSegment = (Segment) obj;
      return otherSegment.partition == this.partition && otherSegment.segmentNumber == this.segmentNumber;
    } else {
      return false;
    }
  }
}
