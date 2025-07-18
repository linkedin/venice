package com.linkedin.venice.utils;

public class SleepStallingMockTime implements Time {
  private static final double INITIAL_WAIT_TIME = 0.1;
  private volatile long currentTimeMillis;
  private double waitTime = INITIAL_WAIT_TIME;

  public SleepStallingMockTime() {
    this.currentTimeMillis = System.currentTimeMillis();
  }

  public SleepStallingMockTime(long currentTimeMillis) {
    this.currentTimeMillis = currentTimeMillis;
  }

  @Override
  public long getMilliseconds() {
    return this.currentTimeMillis;
  }

  @Override
  public long getNanoseconds() {
    return this.currentTimeMillis * Time.NS_PER_MS;
  }

  @Override
  public void sleep(long ms) throws InterruptedException {
    long endTime = this.currentTimeMillis + ms;
    while (this.currentTimeMillis < endTime) {
      /**
       * A simple adaptive mutex... it will spin for a little while and eventually start sleeping with exponential
       * backoff, until the condition is met (which will only happen if a sufficiently large amount is passed into
       * {@link #advanceTime(long)}). The intent is to strike a balance between minimizing test time and CPU overhead.
       */
      synchronized (this) {
        long waitTime = getWaitTime();
        if (waitTime > 0) {
          wait(waitTime);
        }
      }
    }
  }

  private synchronized long getWaitTime() {
    long currentWaitTime = (long) Math.floor(this.waitTime);
    this.waitTime *= 2;
    return currentWaitTime;
  }

  public synchronized void advanceTime(long ms) {
    this.currentTimeMillis = this.currentTimeMillis + ms;
    this.waitTime = INITIAL_WAIT_TIME;
    notifyAll();
  }
}
