package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.throttle.EventThrottler;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceAdaptiveIngestionThrottler extends EventThrottler {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAdaptiveIngestionThrottler.class);
  private final List<BooleanSupplier> limiterSuppliers = new ArrayList<>();
  private final List<BooleanSupplier> boosterSuppliers = new ArrayList<>();

  private int throttlerNum;
  private final List<EventThrottler> eventThrottlers = new ArrayList<>();
  private final int signalIdleThreshold;
  private int signalIdleCount = 0;
  private AtomicInteger currentThrottlerIndex = new AtomicInteger();

  public VeniceAdaptiveIngestionThrottler(
      int signalIdleThreshold,
      long quotaPerSecond,
      List<Double> factors,
      long timeWindow,
      String throttlerName) {
    this.signalIdleThreshold = signalIdleThreshold;
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
      EventThrottler eventThrottler = new EventThrottler(
          (long) (quotaPerSecond * factor),
          timeWindow,
          throttlerName + decimalFormat.format(factor),
          false,
          EventThrottler.BLOCK_STRATEGY);
      eventThrottlers.add(eventThrottler);
    }
  }

  @Override
  public void maybeThrottle(double eventsSeen) {
    eventThrottlers.get(currentThrottlerIndex.get()).maybeThrottle(eventsSeen);
  }

  public void registerLimiterSignal(BooleanSupplier supplier) {
    limiterSuppliers.add(supplier);
  }

  public void registerBoosterSignal(BooleanSupplier supplier) {
    boosterSuppliers.add(supplier);
  }

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
        LOGGER.info(
            "Found active limiter signal, adjusting throttler index to: {} with throttle rate: {}",
            currentThrottlerIndex,
            eventThrottlers.get(currentThrottlerIndex.get()).getMaxRatePerSecond());
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
        LOGGER.info(
            "Found active booster signal, adjusting throttler index to: {} with throttle rate: {}",
            currentThrottlerIndex,
            eventThrottlers.get(currentThrottlerIndex.get()).getMaxRatePerSecond());
      }
    }
    if (isSignalIdle) {
      signalIdleCount += 1;
      LOGGER.info("No active signal found, increasing idle count to {}/{}", signalIdleCount, signalIdleThreshold);
      if (signalIdleCount == signalIdleThreshold) {
        if (currentThrottlerIndex.get() < throttlerNum - 1) {
          currentThrottlerIndex.incrementAndGet();
        }
        LOGGER.info(
            "Reach max signal idle count, adjusting throttler index to: {} with throttle rate: {}",
            currentThrottlerIndex,
            eventThrottlers.get(currentThrottlerIndex.get()).getMaxRatePerSecond());
        signalIdleCount = 0;
      }
    }
  }

  public long getCurrentThrottlerRate() {
    return eventThrottlers.get(currentThrottlerIndex.get()).getMaxRatePerSecond();
  }

  // TEST
  int getCurrentThrottlerIndex() {
    return currentThrottlerIndex.get();
  }
}
