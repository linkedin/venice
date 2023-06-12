package com.linkedin.venice.utils;

/**
 * A {@link Time} implementation which abstracts logical time away from real
 * time, so that it can be deterministically controlled.
 *
 * Useful for tests which are timing-dependent.
 */
public class TestMockTime implements Time, io.tehuti.utils.Time {
  private long timeMs;

  public TestMockTime() {
    this.timeMs = System.currentTimeMillis();
  }

  public TestMockTime(long time) {
    this.timeMs = time;
  }

  public long getMilliseconds() {
    return this.timeMs;
  }

  public long getNanoseconds() {
    return this.timeMs * Time.NS_PER_MS;
  }

  public void sleep(long ms) {
    addMilliseconds(ms);
  }

  public void setTime(long ms) {
    this.timeMs = ms;
  }

  public void addMilliseconds(long ms) {
    this.timeMs += ms;
  }

  /**
   * For interop with Tehuti's Time abstraction
   */
  @Override
  public long milliseconds() {
    return getMilliseconds();
  }

  /**
   * For interop with Tehuti's Time abstraction
   */
  @Override
  public long nanoseconds() {
    return getNanoseconds();
  }
}
