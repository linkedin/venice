package com.linkedin.venice.throttle;

import java.util.function.BooleanSupplier;


/**
 * The {@code VeniceAdaptiveThrottler} interface defines a contract for adaptive throttling mechanisms
 * that can dynamically adjust their throughput limits based on external signals.
 * <p>
 * Implementations are expected to:
 * <ul>
 *   <li>Register limiter signals (conditions that reduce or restrict throughput).</li>
 *   <li>Register booster signals (conditions that allow higher throughput).</li>
 *   <li>Periodically check these signals and adjust the throttler configuration accordingly.</li>
 *   <li>Expose the current effective throttling rate and a name for identification.</li>
 * </ul>
 */
public interface VeniceAdaptiveThrottler {
  /**
   * Registers a limiter signal supplier that, when evaluated to {@code true},
   * indicates that the throttler should reduce its throughput rate.
   *
   * @param supplier a {@link BooleanSupplier} providing the limiter signal.
   */
  void registerLimiterSignal(BooleanSupplier supplier);

  /**
   * Registers a booster signal supplier that, when evaluated to {@code true},
   * indicates that the throttler may safely increase its throughput rate.
   *
   * @param supplier a {@link BooleanSupplier} providing the booster signal.
   */
  void registerBoosterSignal(BooleanSupplier supplier);

  /**
   * Evaluates all registered limiter and booster signals, and adjusts the throttler’s
   * rate accordingly. The exact adjustment strategy is defined by the implementation.
   */
  void checkSignalAndAdjustThrottler();

  /**
   * Returns the current effective throttler rate after adaptive adjustments.
   *
   * @return the current throttling rate, typically represented in operations per second
   *         or another unit defined by the implementation.
   */
  long getCurrentThrottlerRate();

  /**
   * Returns a human-readable name for this throttler instance.
   * <p>
   * This is useful for logging, debugging, or monitoring purposes when multiple throttlers
   * are in use.
   *
   * @return the throttler’s name.
   */
  String getThrottlerName();
}
