package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.stats.AdaptiveThrottlingServiceStats;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.throttle.VeniceAdaptiveThrottler;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceAdaptiveIngestionThrottler extends EventThrottler implements VeniceAdaptiveThrottler {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAdaptiveIngestionThrottler.class);
  private static final long DEFAULT_UNLIMITED_QUOTA_VALUE = -1L;
  private final List<BooleanSupplier> limiterSuppliers = new ArrayList<>();
  private final List<BooleanSupplier> boosterSuppliers = new ArrayList<>();

  private final int throttlerNum;
  private final List<EventThrottler> eventThrottlers = new ArrayList<>();
  private final int signalIdleThreshold;
  private int signalIdleCount = 0;
  private final AtomicInteger currentThrottlerIndex = new AtomicInteger();
  private final String throttlerName;
  private final AdaptiveThrottlingServiceStats adaptiveThrottlingServiceStats;

  public VeniceAdaptiveIngestionThrottler(
      int signalIdleThreshold,
      long quotaPerSecond,
      List<Double> factors,
      long timeWindow,
      String throttlerName,
      AdaptiveThrottlingServiceStats adaptiveThrottlingServiceStats) {
    if (quotaPerSecond == 0) {
      throw new IllegalArgumentException("Can not create throttler with 0 quotaPerSecond");
    }

    this.signalIdleThreshold = signalIdleThreshold;
    this.throttlerName = throttlerName;
    DecimalFormat decimalFormat = new DecimalFormat("0.0");
    throttlerNum = factors.size();
    for (int i = 0; i < throttlerNum; i++) {
      if (factors.get(i) == 1.0D) {
        currentThrottlerIndex.set(i);
        break;
      }
    }
    if (currentThrottlerIndex.get() == -1) {
      throw new IllegalArgumentException("No throttler factor of 1.0D found");
    }

    for (Double factor: factors) {
      long maxQuota = (long) (quotaPerSecond * factor);
      if (quotaPerSecond == DEFAULT_UNLIMITED_QUOTA_VALUE) {
        maxQuota = DEFAULT_UNLIMITED_QUOTA_VALUE;
        LOGGER.warn("Quota per second is not set (-1), setting max quota per throttler to -1");
      }
      EventThrottler eventThrottler = new EventThrottler(
          maxQuota,
          timeWindow,
          throttlerName + decimalFormat.format(factor),
          false,
          EventThrottler.BLOCK_STRATEGY);
      eventThrottlers.add(eventThrottler);
    }
    this.adaptiveThrottlingServiceStats = adaptiveThrottlingServiceStats;
  }

  public String getThrottlerName() {
    return throttlerName;
  }

  @Override
  public void maybeThrottle(double eventsSeen) {
    eventThrottlers.get(currentThrottlerIndex.get()).maybeThrottle(eventsSeen);
    adaptiveThrottlingServiceStats.recordRateForAdaptiveThrottler(this, (int) eventsSeen);
  }

  @Override
  public void registerLimiterSignal(BooleanSupplier supplier) {
    limiterSuppliers.add(supplier);
  }

  @Override
  public void registerBoosterSignal(BooleanSupplier supplier) {
    boosterSuppliers.add(supplier);
  }

  @Override
  public void checkSignalAndAdjustThrottler() {
    boolean isSignalIdle = true;
    boolean hasLimitedRate = false;
    for (BooleanSupplier supplier: limiterSuppliers) {
      if (supplier.getAsBoolean()) {
        hasLimitedRate = true;
        isSignalIdle = false;
        signalIdleCount = 0;
        int currentIndex = currentThrottlerIndex.get();
        // for limiter signal, step down limit in faster rate.
        if (currentIndex >= 1) {
          currentThrottlerIndex.set(Math.max(currentIndex - 2, 0));
        }
        long maxRate = eventThrottlers.get(currentThrottlerIndex.get()).getMaxRatePerSecond();
        // for uninitialized throttlers (rate = 0) do not print logs
        if (maxRate > 0) {
          LOGGER.info(
              "Found limiter signal for: {}, adjusting throttler index to: {} with throttle rate: {}",
              throttlerName,
              currentThrottlerIndex,
              maxRate);
        }
      }
    }
    // If any limiter signal is true do not booster the throttler
    if (hasLimitedRate) {
      return;
    }

    for (BooleanSupplier supplier: boosterSuppliers) {
      if (supplier.getAsBoolean()) {
        isSignalIdle = false;
        signalIdleCount = 0;
        if (currentThrottlerIndex.get() < throttlerNum - 1) {
          currentThrottlerIndex.incrementAndGet();
        }
        long maxRate = eventThrottlers.get(currentThrottlerIndex.get()).getMaxRatePerSecond();
        // for uninitialized throttlers (rate = 0) do not print logs
        if (maxRate > 0) {
          LOGGER.info(
              "Found booster signal for: {}, adjusting throttler index to: {} with throttle rate: {}",
              throttlerName,
              currentThrottlerIndex,
              maxRate);
        }
      }
    }
    if (isSignalIdle) {
      signalIdleCount += 1;
      if (signalIdleCount == signalIdleThreshold) {
        if (currentThrottlerIndex.get() < throttlerNum - 1) {
          currentThrottlerIndex.incrementAndGet();
        }
        LOGGER.info(
            "Reach max signal idle count: {} for: {}, adjusting throttler index to: {} with throttle rate: {}",
            signalIdleThreshold,
            throttlerName,
            currentThrottlerIndex,
            eventThrottlers.get(currentThrottlerIndex.get()).getMaxRatePerSecond());
        signalIdleCount = 0;
      }
    }
  }

  @Override
  public long getCurrentThrottlerRate() {
    return eventThrottlers.get(currentThrottlerIndex.get()).getMaxRatePerSecond();
  }

  // TEST
  int getCurrentThrottlerIndex() {
    return currentThrottlerIndex.get();
  }
}
