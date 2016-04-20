package com.linkedin.venice.utils;

import java.util.Date;

/**
 * The interface to time itself. Oh wow, your head totally just exploded.
 */
public interface Time {
  long HOURS_PER_DAY = 24;
  long US_PER_MS = 1000;
  long NS_PER_US = 1000;
  long NS_PER_MS = US_PER_MS * NS_PER_US;
  long MS_PER_SECOND = 1000;
  long US_PER_SECOND = US_PER_MS * MS_PER_SECOND;
  long NS_PER_SECOND = NS_PER_US * US_PER_SECOND;
  long SECONDS_PER_MINUTE = 60;
  long MINUTES_PER_HOUR = 60;
  long SECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE;
  long SECONDS_PER_DAY = HOURS_PER_DAY * SECONDS_PER_HOUR;
  long MS_PER_MINUTE = SECONDS_PER_MINUTE * MS_PER_SECOND;
  long MS_PER_HOUR = SECONDS_PER_HOUR * MS_PER_SECOND;
  long MS_PER_DAY = SECONDS_PER_DAY * MS_PER_SECOND;

  long getMilliseconds();

  long getNanoseconds();

  int getSeconds();

  Date getCurrentDate();

  void sleep(long ms) throws InterruptedException;
}
