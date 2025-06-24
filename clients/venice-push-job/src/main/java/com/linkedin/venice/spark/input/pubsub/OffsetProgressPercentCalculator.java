package com.linkedin.venice.spark.input.pubsub;

public class OffsetProgressPercentCalculator {
  private final long startingOffset;
  private final long endingOffset;
  private final long offsetLength;

  public OffsetProgressPercentCalculator(long startingOffset, long endingOffset) {
    this.startingOffset = startingOffset;
    this.endingOffset = endingOffset;
    this.offsetLength = endingOffset - startingOffset;
  }

  public float calculate(Long currentOffset) {
    // Return 0.0f for null offset
    if (currentOffset == null) {
      return 0.0f;
    }

    // Return 0.0f if the offset is less than the starting offset
    if (currentOffset < startingOffset) {
      return 0.0f;
    }

    // Return 100.0f if the offset is greater than or equal to the ending offset
    if (currentOffset >= endingOffset) {
      return 100.0f;
    }

    // For consecutive offsets (difference of 1), when currentOffset equals startingOffset, return 50.0f
    if (offsetLength == 1 && currentOffset == startingOffset) {
      return 50.0f;
    }

    // Calculate percentage for offsets between start and end
    return (float) (currentOffset - startingOffset) * 100.0f / (float) offsetLength;
  }
}
