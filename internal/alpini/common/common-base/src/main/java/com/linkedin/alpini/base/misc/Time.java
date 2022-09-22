package com.linkedin.alpini.base.misc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.LongSupplier;


/**
 * Utility methods for time travelling and freezing time.
 * This is useful when implementing tests which are dependant upon the
 * passage of time.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum Time {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  private volatile Pair<Long, Long> _frozenTime;
  private volatile LongSupplier _timeSource = System::currentTimeMillis;
  private volatile LongSupplier _nanoSource = System::nanoTime;

  private long getCurrentTimeMillis() {
    return _timeSource.getAsLong();
  }

  private long getNanoTime() {
    return _nanoSource.getAsLong();
  }

  private synchronized void restoreTime() {
    _timeSource = System::currentTimeMillis;
    _nanoSource = System::nanoTime;
    _frozenTime = null;
  }

  public static long currentTimeMillis() {
    return SINGLETON.getCurrentTimeMillis();
  }

  public static long nanoTime() {
    return SINGLETON.getNanoTime();
  }

  public static void restore() {
    SINGLETON.restoreTime();
  }

  private static long frozenTimeMillis() {
    Pair<Long, Long> frozenTime = SINGLETON._frozenTime;
    return frozenTime != null ? frozenTime.getFirst() : System.currentTimeMillis();
  }

  private static long frozenNanoTime() {
    Pair<Long, Long> frozenTime = SINGLETON._frozenTime;
    return frozenTime != null ? frozenTime.getSecond() : System.nanoTime();
  }

  private void freezeTime() {
    freeze(_timeSource, _nanoSource);
  }

  public static void freeze() {
    SINGLETON.freezeTime();
  }

  public static void freeze(long frozenTimeMillis, long frozenNano) {
    SINGLETON.freeze(() -> frozenTimeMillis, () -> frozenNano);
  }

  private synchronized void freezeTime(LongSupplier frozenTime, LongSupplier frozenNano) {
    _timeSource = Time::frozenTimeMillis;
    _nanoSource = Time::frozenNanoTime;
    _frozenTime = Pair.make(frozenTime.getAsLong(), frozenNano.getAsLong());
    notifyAll();
  }

  private void freeze(LongSupplier frozenTime, LongSupplier frozenNano) {
    SINGLETON.freezeTime(Objects.requireNonNull(frozenTime), Objects.requireNonNull(frozenNano));
  }

  private synchronized void advanceTime(long time, TimeUnit unit) {
    LongSupplier oldTimeSource = _timeSource;
    LongSupplier oldNanoSource = _nanoSource;
    _timeSource = () -> oldTimeSource.getAsLong() + unit.toMillis(time);
    _nanoSource = () -> oldNanoSource.getAsLong() + unit.toNanos(time);
    notifyAll();
  }

  public static void advance(long time, TimeUnit unit) {
    Objects.requireNonNull(unit);
    if (time > 0) {
      SINGLETON.advanceTime(time, unit);
    }
  }

  /**
   * Obtains a clock that returns the current instant using the best available
   * system clock, converting to date and time using the UTC time-zone.
   * <p>
   * This clock, rather than {@link #systemDefaultZone()}, should be used when
   * you need the current instant without the date or time.
   * <p>
   * This clock is based on the best available system clock.
   * This may use {@link System#currentTimeMillis()}, or a higher resolution
   * clock if one is available.
   * <p>
   * Conversion from instant to date or time uses the {@linkplain ZoneOffset#UTC UTC time-zone}.
   * <p>
   * The returned implementation is immutable, thread-safe and {@code Serializable}.
   * It is equivalent to {@code system(ZoneOffset.UTC)}.
   *
   * @return a clock that uses the best available system clock in the UTC zone, not null
   */
  public static Clock systemUTC() {
    return system(ZoneOffset.UTC);
  }

  /**
   * Obtains a clock that returns the current instant using the best available
   * system clock, converting to date and time using the default time-zone.
   * <p>
   * This clock is based on the best available system clock.
   * This may use {@link System#currentTimeMillis()}, or a higher resolution
   * clock if one is available.
   * <p>
   * Using this method hard codes a dependency to the default time-zone into your application.
   * It is recommended to avoid this and use a specific time-zone whenever possible.
   * The {@link #systemUTC() UTC clock} should be used when you need the current instant
   * without the date or time.
   * <p>
   * The returned implementation is immutable, thread-safe and {@code Serializable}.
   * It is equivalent to {@code system(ZoneId.systemDefault())}.
   *
   * @return a clock that uses the best available system clock in the default zone, not null
   * @see ZoneId#systemDefault()
   */
  public static Clock systemDefaultZone() {
    return system(ZoneId.systemDefault());
  }

  /**
   * Obtains a clock that returns the current instant using best available
   * system clock.
   * <p>
   * This clock is based on the best available system clock.
   * This may use {@link System#currentTimeMillis()}, or a higher resolution
   * clock if one is available.
   * <p>
   * Conversion from instant to date or time uses the specified time-zone.
   * <p>
   * The returned implementation is immutable, thread-safe and {@code Serializable}.
   *
   * @param zone  the time-zone to use to convert the instant to date-time, not null
   * @return a clock that uses the best available system clock in the specified zone, not null
   */
  public static Clock system(ZoneId zone) {
    Objects.requireNonNull(zone, "zone");
    return new SystemClock(zone);
  }

  private static class SystemClock extends Clock {
    private final ZoneId _zone;

    SystemClock(ZoneId zone) {
      this._zone = zone;
    }

    @Override
    public ZoneId getZone() {
      return _zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      if (zone.equals(this._zone)) { // intentional NPE
        return this;
      }
      return new SystemClock(zone);
    }

    @Override
    public long millis() {
      return currentTimeMillis();
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(millis());
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof SystemClock && _zone.equals(((SystemClock) obj)._zone);
    }

    @Override
    public int hashCode() {
      return _zone.hashCode() + 1;
    }

    @Override
    public String toString() {
      return "SystemClock[" + _zone + "]";
    }
  }

  private synchronized void sleepWait(long milliSeconds, int nanos) throws InterruptedException {
    wait(milliSeconds, nanos);
  }

  private static void sleep0(long milliSeconds, int nanos) throws InterruptedException {
    SINGLETON.sleepWait(milliSeconds, nanos);
  }

  private static void sleep1(long milliSeconds, int nanos) {
    try {
      sleep0(milliSeconds, nanos);
    } catch (InterruptedException ignored) {
      // ignored
    }
  }

  public static void sleep(long milliseconds) throws InterruptedException {
    sleep(Time::sleep0, milliseconds, 0);
  }

  public static void sleep(long milliseconds, int nanos) throws InterruptedException {
    sleep(Time::sleep0, milliseconds, nanos);
  }

  private static void sleep(Sleep s, long milliseconds, int nanos) throws InterruptedException {
    final long startTimeNanos = nanoTime();
    final long endTimeNanos = startTimeNanos + TimeUnit.MILLISECONDS.toNanos(milliseconds) + nanos;
    if (endTimeNanos > startTimeNanos) {
      long currentTime = startTimeNanos;
      do {
        long delta = endTimeNanos - currentTime;
        long millis = TimeUnit.NANOSECONDS.toMillis(delta);
        delta -= TimeUnit.MILLISECONDS.toNanos(millis);
        s.sleep(millis, (int) delta);
        currentTime = nanoTime();
      } while (currentTime >= startTimeNanos && currentTime < endTimeNanos);
    }
  }

  public static void sleepUninterruptably(long milliseconds) {
    sleepInterruptable(Time::sleep1, milliseconds);
  }

  public static boolean sleepInterruptable(long milliseconds) {
    return sleepInterruptable(Time::sleep0, milliseconds);
  }

  private static boolean sleepInterruptable(Sleep s, long milliSeconds) {
    boolean interrupted = false;
    try {
      sleep(s, milliSeconds, 0);
    } catch (InterruptedException ignored) {
      interrupted = true;
    }
    return interrupted || Thread.interrupted();
  }

  private interface Sleep {
    void sleep(long milliseconds, int nanos) throws InterruptedException;
  }

  public static boolean await(CountDownLatch latch, long timeout, TimeUnit unit) throws InterruptedException {
    return await(latch::await, timeout, unit);
  }

  public static boolean await(Condition latch, long timeout, TimeUnit unit) throws InterruptedException {
    return await(latch::await, timeout, unit);
  }

  public static boolean await(Awaitable obj, long timeout, TimeUnit unit) throws InterruptedException {
    final long millis = unit.toMillis(timeout);
    if (millis > 0) {
      final long startTimeMillis = currentTimeMillis();
      final long endTimeMillis = startTimeMillis + millis;
      boolean completed;
      long currentTime;
      do {
        completed = obj.await(1, TimeUnit.MILLISECONDS);
        currentTime = currentTimeMillis();
      } while (!completed && currentTime >= startTimeMillis && currentTime < endTimeMillis);
      return completed;
    } else {
      return obj.await(timeout, unit);
    }
  }

  public interface Awaitable {
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;
  }
}
