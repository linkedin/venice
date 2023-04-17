package com.linkedin.venice.utils;

/**
 * The interface to time itself. Oh wow, your head totally just exploded.
 */
public interface Time {
  int HOURS_PER_DAY = 24;
  int US_PER_MS = 1000;
  int NS_PER_US = 1000;
  int NS_PER_MS = US_PER_MS * NS_PER_US;
  int MS_PER_SECOND = 1000;
  int US_PER_SECOND = US_PER_MS * MS_PER_SECOND;
  int NS_PER_SECOND = NS_PER_US * US_PER_SECOND;
  int SECONDS_PER_MINUTE = 60;
  int MINUTES_PER_HOUR = 60;
  int SECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE;
  int SECONDS_PER_DAY = HOURS_PER_DAY * SECONDS_PER_HOUR;
  int MS_PER_MINUTE = SECONDS_PER_MINUTE * MS_PER_SECOND;
  int MS_PER_HOUR = SECONDS_PER_HOUR * MS_PER_SECOND;
  int MS_PER_DAY = SECONDS_PER_DAY * MS_PER_SECOND;

  long getMilliseconds();

  long getNanoseconds();

  void sleep(long ms) throws InterruptedException;
}
