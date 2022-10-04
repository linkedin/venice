package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.registry.Shutdownable;
import io.netty.util.HashedWheelTimer;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ShutdownableHashedWheelTimer extends HashedWheelTimer implements Shutdownable {
  /**
   * Creates a new timer with the default thread factory
   * ({@link java.util.concurrent.Executors#defaultThreadFactory()}), default tick duration, and
   * default number of ticks per wheel.
   */
  public ShutdownableHashedWheelTimer() {
  }

  /**
   * Creates a new timer with the default thread factory
   * ({@link java.util.concurrent.Executors#defaultThreadFactory()}) and default number of ticks
   * per wheel.
   *
   * @param tickDuration the duration between tick
   * @param unit         the time unit of the {@code tickDuration}
   * @throws NullPointerException     if {@code unit} is {@code null}
   * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
   */
  public ShutdownableHashedWheelTimer(long tickDuration, TimeUnit unit) {
    super(tickDuration, unit);
  }

  /**
   * Creates a new timer with the default thread factory
   * ({@link java.util.concurrent.Executors#defaultThreadFactory()}).
   *
   * @param tickDuration  the duration between tick
   * @param unit          the time unit of the {@code tickDuration}
   * @param ticksPerWheel the size of the wheel
   * @throws NullPointerException     if {@code unit} is {@code null}
   * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
   */
  public ShutdownableHashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
    super(tickDuration, unit, ticksPerWheel);
  }

  /**
   * Creates a new timer with the default tick duration and default number of
   * ticks per wheel.
   *
   * @param threadFactory a {@link ThreadFactory} that creates a
   *                      background {@link Thread} which is dedicated to
   *                      {@link java.util.TimerTask} execution.
   * @throws NullPointerException if {@code threadFactory} is {@code null}
   */
  public ShutdownableHashedWheelTimer(ThreadFactory threadFactory) {
    super(threadFactory);
  }

  /**
   * Creates a new timer with the default number of ticks per wheel.
   *
   * @param threadFactory a {@link ThreadFactory} that creates a
   *                      background {@link Thread} which is dedicated to
   *                      {@link java.util.TimerTask} execution.
   * @param tickDuration  the duration between tick
   * @param unit          the time unit of the {@code tickDuration}
   * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
   * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
   */
  public ShutdownableHashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
    super(threadFactory, tickDuration, unit);
  }

  /**
   * Creates a new timer.
   *
   * @param threadFactory a {@link ThreadFactory} that creates a
   *                      background {@link Thread} which is dedicated to
   *                      {@link java.util.TimerTask} execution.
   * @param tickDuration  the duration between tick
   * @param unit          the time unit of the {@code tickDuration}
   * @param ticksPerWheel the size of the wheel
   * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
   * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
   */
  public ShutdownableHashedWheelTimer(
      ThreadFactory threadFactory,
      long tickDuration,
      TimeUnit unit,
      int ticksPerWheel) {
    super(threadFactory, tickDuration, unit, ticksPerWheel);
  }

  /**
   * Creates a new timer.
   *
   * @param threadFactory a {@link ThreadFactory} that creates a
   *                      background {@link Thread} which is dedicated to
   *                      {@link java.util.TimerTask} execution.
   * @param tickDuration  the duration between tick
   * @param unit          the time unit of the {@code tickDuration}
   * @param ticksPerWheel the size of the wheel
   * @param leakDetection {@code true} if leak detection should be enabled always, if false it will only be enabled
   *                      if the worker thread is not a daemon thread.
   * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
   * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
   */
  public ShutdownableHashedWheelTimer(
      ThreadFactory threadFactory,
      long tickDuration,
      TimeUnit unit,
      int ticksPerWheel,
      boolean leakDetection) {
    super(threadFactory, tickDuration, unit, ticksPerWheel, leakDetection);
  }

  @Override
  public void shutdown() {
    stop();
  }

  @Override
  public void waitForShutdown() throws InterruptedException, IllegalStateException {
  }

  @Override
  public void waitForShutdown(long timeoutInMs) throws InterruptedException, IllegalStateException, TimeoutException {
  }
}
