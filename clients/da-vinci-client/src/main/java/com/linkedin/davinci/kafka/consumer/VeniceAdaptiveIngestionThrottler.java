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

  private int throttlerNum;
  private final List<EventThrottler> eventThrottlers = new ArrayList<>();
  private final int signalIdleThreshold;
  private int signalIdleCount = 0;
  private int currentThrottlerIndex = -1;

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
      Double factor = factors.get(i);
      if (factor == 1.0D) {
        currentThrottlerIndex = i;
      }
      EventThrottler eventThrottler = new EventThrottler(
          (long) (quotaPerSecond * factor),
          timeWindow,
          throttlerName + decimalFormat.format(factors.get(i)),
          false,
          EventThrottler.BLOCK_STRATEGY);
      eventThrottlers.add(eventThrottler);
    }
    if (currentThrottlerIndex == -1) {
      throw new IllegalArgumentException("No throttler factor of 1.0D found");
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
        LOGGER.info(
            "Found active limiter signal, adjusting throttler index to: {} with throttle rate: {}",
            currentThrottlerIndex,
            eventThrottlers.get(currentThrottlerIndex).getMaxRatePerSecond());
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
        if (currentThrottlerIndex < throttlerNum - 1) {
          currentThrottlerIndex++;
        }
        LOGGER.info(
            "Found active booster signal, adjusting throttler index to: {} with throttle rate: {}",
            currentThrottlerIndex,
            eventThrottlers.get(currentThrottlerIndex).getMaxRatePerSecond());
      }
    }
    if (isSignalIdle) {
      signalIdleCount += 1;
      LOGGER.info("No active signal found, increasing idle count to {}/{}", signalIdleCount, signalIdleThreshold);
      if (signalIdleCount == signalIdleThreshold) {
        if (currentThrottlerIndex < throttlerNum - 1) {
          currentThrottlerIndex++;
        }
        LOGGER.info(
            "Reach max signal idle count, adjusting throttler index to: {} with throttle rate: {}",
            currentThrottlerIndex,
            eventThrottlers.get(currentThrottlerIndex).getMaxRatePerSecond());
        signalIdleCount = 0;
      }
    }
  }

  public long getCurrentThrottlerRate() {
    return eventThrottlers.get(currentThrottlerIndex).getMaxRatePerSecond();
  }

  // TEST
  int getCurrentThrottlerIndex() {
    return currentThrottlerIndex;
  }
}
