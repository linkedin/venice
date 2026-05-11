package com.linkedin.venice.utils;

public class SleepStallingMockTime implements Time {
  private static final double INITIAL_WAIT_TIME = 0.1;
  private volatile long currentTimeMillis;
  private double waitTime = INITIAL_WAIT_TIME;
  /*
   * Counts how many sleep() calls have entered the wait loop and not yet exited. Lets test
   * code call awaitSleeper() to wait until at least one task thread is parked in sleep()
   * before calling advanceTime(), eliminating the "advance lost before sleep" race.
   */
  private volatile int sleepersInWait = 0;

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
          sleepersInWait++;
          notifyAll(); // wake awaitSleeper() callers
          try {
            wait(waitTime);
          } finally {
            sleepersInWait--;
          }
        }
      }
    }
  }

  /**
   * Block until at least one thread is parked in sleep()'s wait loop. Test helpers should
   * call this before advanceTime() to avoid the "advance lost before sleep" race where the
   * advancement is silently absorbed by the next sleep's endTime computation.
   */
  public synchronized void awaitSleeper(long timeoutMillis) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (sleepersInWait == 0) {
      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0) {
        throw new IllegalStateException("No sleeper appeared within " + timeoutMillis + "ms");
      }
      wait(remaining);
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
