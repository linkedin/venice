package com.linkedin.venice.utils;

import java.util.Date;


/**
 * {@link Time} implementation that just reads from the system clock
 */
public class SystemTime implements Time {
  public static final SystemTime INSTANCE = new SystemTime();

  public Date getCurrentDate() {
    return new Date();
  }

  public long getMilliseconds() {
    return System.currentTimeMillis();
  }

  public long getNanoseconds() {
    return System.nanoTime();
  }

  public int getSeconds() {
    return (int) (getMilliseconds() / MS_PER_SECOND);
  }

  public void sleep(long ms) throws InterruptedException {
    Thread.sleep(ms);
  }
}
