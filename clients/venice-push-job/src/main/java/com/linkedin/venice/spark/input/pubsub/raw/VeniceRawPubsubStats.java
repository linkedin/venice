package com.linkedin.venice.spark.input.pubsub.raw;

/**
 * Encapsulates statistics for the Venice Raw Pubsub Input Partition Reader.
 * Tracks metrics about record processing during partition reading.
 */
public class VeniceRawPubsubStats {
  private long recordsServed;
  private long recordsSkipped;
  private long recordsDeliveredByGet;

  /**
   * Creates a new stats object with zero-initialized metrics.
   */
  public VeniceRawPubsubStats() {
    this(0, 0, 0);
  }

  /**
   * Creates a new stats object with the given metrics.
   *
   * @param recordsServed The number of records successfully served
   * @param recordsSkipped The number of records skipped (e.g., control messages)
   * @param recordsDeliveredByGet The number of times the get() method was called
   */
  public VeniceRawPubsubStats(long recordsServed, long recordsSkipped, long recordsDeliveredByGet) {
    this.recordsServed = recordsServed;
    this.recordsSkipped = recordsSkipped;
    this.recordsDeliveredByGet = recordsDeliveredByGet;
  }

  /**
   * @return The number of records successfully served
   */
  public long getRecordsServed() {
    return recordsServed;
  }

  /**
   * @return The number of records skipped (e.g., control messages)
   */
  public long getRecordsSkipped() {
    return recordsSkipped;
  }

  /**
   * @return The number of times the get() method was called
   */
  public long getRecordsDeliveredByGet() {
    return recordsDeliveredByGet;
  }

  /**
   * Increments the count of records served by 1.
   */
  public void incrementRecordsServed() {
    recordsServed++;
  }

  /**
   * Increments the count of records skipped by 1.
   */
  public void incrementRecordsSkipped() {
    recordsSkipped++;
  }

  /**
   * Increments the count of records delivered by get() by 1.
   */
  public void incrementRecordsDeliveredByGet() {
    recordsDeliveredByGet++;
  }

  @Override
  public String toString() {
    return "VeniceRawPubsubStats{" + "recordsServed=" + recordsServed + ", recordsSkipped=" + recordsSkipped
        + ", recordsDeliveredByGet=" + recordsDeliveredByGet + '}';
  }
}
