package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.throttle.EventThrottler;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceAdaptiveIngestionThrottler extends EventThrottler {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAdaptiveIngestionThrottler.class);
  private final List<BooleanSupplier> limiterSuppliers = new ArrayList<>();
  private final List<BooleanSupplier> boosterSuppliers = new ArrayList<>();

  private final static int MAX_THROTTLERS = 7;
  private final List<EventThrottler> eventThrottlers = new ArrayList<>(MAX_THROTTLERS);
  private final int signalIdleThreshold;
  private int signalIdleCount = 0;
  private int currentThrottlerIndex = MAX_THROTTLERS / 2;

  public VeniceAdaptiveIngestionThrottler(
      int signalIdleThreshold,
      long quotaPerSecond,
      long timeWindow,
      String throttlerName) {
    this.signalIdleThreshold = signalIdleThreshold;
    double factor = 0.4;
    DecimalFormat decimalFormat = new DecimalFormat("0.0");
    for (int i = 0; i < MAX_THROTTLERS; i++) {
      EventThrottler eventThrottler = new EventThrottler(
          (long) (quotaPerSecond * factor),
          timeWindow,
          throttlerName + decimalFormat.format(factor),
          false,
          EventThrottler.BLOCK_STRATEGY);
      eventThrottlers.add(eventThrottler);
      factor = factor + 0.2;
    }
  }

  @Override
  public void maybeThrottle(double eventsSeen) {
    eventThrottlers.get(currentThrottlerIndex).maybeThrottle(eventsSeen);
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
        if (currentThrottlerIndex > 0) {
          currentThrottlerIndex--;
        }
        LOGGER.info("Found active limiter signal, adjusting throttler index to {}", currentThrottlerIndex);
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
        if (currentThrottlerIndex < MAX_THROTTLERS - 1) {
          currentThrottlerIndex++;
        }
        LOGGER.info("Found active booster signal, adjusting throttler index to {}", currentThrottlerIndex);
      }
    }
    if (isSignalIdle) {
      signalIdleCount += 1;
      LOGGER.info("No active signal found, increasing idle count to {}/{}", signalIdleCount, signalIdleThreshold);
      if (signalIdleCount == signalIdleThreshold) {
        if (currentThrottlerIndex < MAX_THROTTLERS - 1) {
          currentThrottlerIndex++;
        }
        LOGGER.info("Reach max signal idle count, adjusting throttler index to {}", currentThrottlerIndex);
        signalIdleCount = 0;
      }
    }
  }

  // TEST
  int getCurrentThrottlerIndex() {
    return currentThrottlerIndex;
  }
}
