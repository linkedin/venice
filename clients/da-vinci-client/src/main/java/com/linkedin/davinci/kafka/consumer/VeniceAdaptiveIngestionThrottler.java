package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.throttle.EventThrottler;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;


public class VeniceAdaptiveIngestionThrottler extends EventThrottler {
  private final List<BooleanSupplier> limiterSuppliers = new ArrayList<>();
  private final List<BooleanSupplier> boosterSuppliers = new ArrayList<>();

  private final int MAX_THROTTLERS = 7;
  private final List<EventThrottler> eventThrottlers = new ArrayList<>(MAX_THROTTLERS);

  private int currentThrottlerIndex = 3;

  public VeniceAdaptiveIngestionThrottler(int quotaPerSecond, long timeWindow, String throttlerName) {
    double factor = 0.4;
    DecimalFormat decimalFormat = new DecimalFormat("0.0");
    for (int i = 0; i < 7; i++) {
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

  private void checkSignalAndAdjustThrottler() {
    boolean hasLimitedRate = false;
    for (BooleanSupplier supplier: limiterSuppliers) {
      if (supplier.getAsBoolean()) {
        hasLimitedRate = true;
        if (currentThrottlerIndex > 0) {
          currentThrottlerIndex--;
        }
      }
    }
    // If any limiter signal is true do not booster the throttler
    if (hasLimitedRate) {
      return;
    }

    for (BooleanSupplier supplier: boosterSuppliers) {
      if (supplier.getAsBoolean()) {
        if (currentThrottlerIndex < MAX_THROTTLERS - 1) {
          currentThrottlerIndex++;
        }
      }
    }
  }
}
