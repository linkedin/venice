package com.linkedin.venice.utils;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Time} implementation which abstracts logical time away from real
 * time, so that it can be deterministically controlled.
 *
 * Useful for tests which are timing-dependent.
 */
public class MockTime implements Time {

  private long timeMs;

  public MockTime() {
    this.timeMs = System.currentTimeMillis();
  }

  public MockTime(long time) {
    this.timeMs = time;
  }

  public Date getCurrentDate() {
    return new Date(timeMs);
  }

  public long getMilliseconds() {
    return this.timeMs;
  }

  public long getNanoseconds() {
    return this.timeMs * Time.NS_PER_MS;
  }

  public int getSeconds() {
    return (int) (timeMs / MS_PER_SECOND);
  }

  public void sleep(long ms) {
    addMilliseconds(ms);
  }

  public void setTime(long ms) {
    this.timeMs = ms;
  }

  public void setCurrentDate(Date date) {
    this.timeMs = date.getTime();
  }

  public void addMilliseconds(long ms) {
    this.timeMs += ms;
  }

  public void add(long duration, TimeUnit units) {
    this.timeMs += TimeUnit.MILLISECONDS.convert(duration, units);
  }
}
