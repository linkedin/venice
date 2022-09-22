package com.linkedin.alpini.base.misc;

/**
 * A non-thread safe counter that should be used and increased in a single thread env when the increment is called
 * @author solu
 * Date: 4/26/21
 */
public class RetryCounter {
  private final long _timestampInSecond;
  private long _retryCount;
  private long _totalCount;

  /**
   * Return current second starting based on System.nanoTime. It is good for a short period comparison.
   * @return
   */
  public static long getCurrentSecond() {
    return Time.nanoTime() / 1000_000_000;
  }

  public RetryCounter() {
    this._timestampInSecond = getCurrentSecond();
  }

  public void increment(boolean isRetry) {
    if (isRetry) {
      _retryCount++;
    }
    _totalCount++;
  }

  public long getRetryCount() {
    return _retryCount;
  }

  public long getTotalCount() {
    return _totalCount;
  }

  @Override
  public String toString() {
    return String.format("[Timestamp=%d, retryCount=%d, totalCount=%d]", _timestampInSecond, _retryCount, _totalCount);
  }

  public long getTimestamp() {
    return _timestampInSecond;
  }
}
