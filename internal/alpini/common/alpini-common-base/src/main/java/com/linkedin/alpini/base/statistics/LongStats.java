package com.linkedin.alpini.base.statistics;

/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface LongStats {
  /**
   * Returns the count.
   * @return the count.
   */
  default int getCount() {
    return Math.toIntExact(getLongCount());
  }

  long getLongCount();

  /**
   * Returns the average.
   * @return the average or {@code NaN} if there is no data.
   */
  double getAverage();

  /**
   * Returns the standard deviation.
   * @return the standard deviation or {@code NaN} if there is no data.
   */
  double getStandardDeviation();

  /**
   * Returns the minimum.
   * @return the minimum or {@code null} if there is no data.
   */
  Long getMinimum();

  /**
   * Returns the maximum value.
   * @return the maximum value or {@code null} if there is no data.
   */
  Long getMaximum();

  /**
   * Returns the 50th percentile value.
   * @return the 50th percentile value or {@code null} if there is no data.
   */
  Long get50Pct();

  /**
   * Returns the 90th percentile value.
   * @return the 90th percentile value or {@code null} if there is no data.
   */
  Long get90Pct();

  /**
   * Returns the 95th percentile value.
   * @return the 95th percentile value or {@code null} if there is no data.
   */
  Long get95Pct();

  /**
   * Returns the 99th percentile value.
   * @return the 99th percentile value or {@code null} if there is no data.
   */
  Long get99Pct();

  /**
   * Returns the 99.9th percentile value.
   * @return the 99.9th percentile value or {@code null} if there is no data.
   */
  Long get99_9Pct(); // SUPPRESS CHECKSTYLE MethodName
}
