package com.linkedin.venice.throttle;

import java.util.function.BooleanSupplier;


public interface VeniceAdaptiveThrottler {
  void registerLimiterSignal(BooleanSupplier supplier);

  void registerBoosterSignal(BooleanSupplier supplier);

  void checkSignalAndAdjustThrottler();

  long getCurrentThrottlerRate();

  String getThrottlerName();
}
