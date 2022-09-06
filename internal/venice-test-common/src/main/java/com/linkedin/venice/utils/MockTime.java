package com.linkedin.venice.utils;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A {@link Time} implementation which abstracts logical time away from real
 * time, so that it can be deterministically controlled.
 *
 * Useful for tests which are timing-dependent.
 */
public class MockTime extends org.apache.kafka.common.utils.MockTime implements org.apache.kafka.common.utils.Time, // For
                                                                                                                    // interop
                                                                                                                    // with
                                                                                                                    // the
                                                                                                                    // Kafka
                                                                                                                    // Broker's
                                                                                                                    // Time
                                                                                                                    // abstraction
    Time, io.tehuti.utils.Time {
  private static final Logger LOGGER = LogManager.getLogger(MockTime.class);

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
    return (int) (timeMs / Time.MS_PER_SECOND);
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

  /**
   * For interop with the Kafka Broker's Time abstraction
   */
  @Override
  public long milliseconds() {
    long time = getMilliseconds();
    LOGGER.debug("Kafka asked for milliseconds. Returned: {}", time);
    return time;
  }

  @Override
  public long hiResClockMs() {
    long time = TimeUnit.NANOSECONDS.toMillis(getNanoseconds());
    LOGGER.debug("Kafka asked for high resolution milliseconds. Returned: {}", time);
    return time;
  }

  /**
   * For interop with the Kafka Broker's Time abstraction
   */
  @Override
  public long nanoseconds() {
    long time = getNanoseconds();
    LOGGER.debug("Kafka asked for nanoseconds. Returned: {}", time);
    return time;
  }
}
