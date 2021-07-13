package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * A mock time that takes a list of synthetic timestamps in MS and return them one by one in a circular manner.
 */
public class MockCircularTime implements Time {
  private List<Long> timestampsInCircular;
  private int currentTimeIndex;

  public MockCircularTime(List<Long> timestampsInMs) {
    if (timestampsInMs == null || timestampsInMs.size() == 0) {
      throw new VeniceException("MockCircularTime class must be initialized with at least one timestamp");
    }
    this.timestampsInCircular = timestampsInMs;
    this.currentTimeIndex = 0;
  }

  public Date getCurrentDate() {
    return new Date(getMilliseconds());
  }

  public long getMilliseconds() {
    long nextTimestampInMs = getNextTimestampInMs();
    return nextTimestampInMs;
  }

  public long getNanoseconds() {
    long nextTimestampInMs = getNextTimestampInMs();
    return TimeUnit.MILLISECONDS.toNanos(nextTimestampInMs);
  }

  public int getSeconds() {
    return (int) (getMilliseconds() / MS_PER_SECOND);
  }

  public void sleep(long ms) throws InterruptedException {
    Thread.sleep(ms);
  }

  private long getNextTimestampInMs() {
    long nextTimestamp = timestampsInCircular.get(currentTimeIndex);
    currentTimeIndex++;
    if (currentTimeIndex >= timestampsInCircular.size()) {
      currentTimeIndex %= timestampsInCircular.size();
    }
    return nextTimestamp;
  }
}
