package com.linkedin.alpini.base.statistics;

public interface LongStatsLowQuantile {
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
   * Returns the 0.1th percentile value.
   * @return the 0.1th percentile value or {@code null} if there is no data.
   */
  Long get01Pct();

  /**
   * Returns the 1th percentile value.
   * @return the 1th percentile value or {@code null} if there is no data.
   */
  Long get1Pct();

  /**
   * Returns the 5th percentile value.
   * @return the 5th percentile value or {@code null} if there is no data.
   */
  Long get5Pct();

  /**
   * Returns the 10th percentile value.
   * @return the 10th percentile value or {@code null} if there is no data.
   */
  Long get10Pct();

  /**
   * Returns the 50th percentile value.
   * @return the 50th percentile value or {@code null} if there is no data.
   */
  Long get50Pct(); // SUPPRESS CHECKSTYLE MethodName
}
