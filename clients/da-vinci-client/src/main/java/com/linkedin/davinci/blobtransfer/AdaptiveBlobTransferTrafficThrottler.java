package com.linkedin.davinci.blobtransfer;

import com.linkedin.venice.throttle.VeniceAdaptiveThrottler;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class adds adaptive throttling control layer for {@link GlobalChannelTrafficShapingHandler}.
 * It controls either global read or write throughput for the blob transfer behavior based on results of the registered
 * limiter and booster signals.
 * The heuristic behavior rules are defined as:
 * (1) If any limiter signal found, it will decrease by 20%. (min 20% of the base rate).
 * (2) If no limiter signal found, but there is booster signal found, it will increase by 20%. (max 200% of the base rate).
 * (3) If none of the above rule applies, it will increase idle count, if idle count is greater than the pre-defined
 * threshold, it will try to increase by 20% (still max 200% of the base rate).
 *
 */
public class AdaptiveBlobTransferTrafficThrottler implements VeniceAdaptiveThrottler {
  private static final Logger LOGGER = LogManager.getLogger(AdaptiveBlobTransferTrafficThrottler.class);
  private static final String THROTTLER_NAME_SUFFIX = "TrafficBlobTransferThrottler";
  private final String throttlerName;
  private final List<BooleanSupplier> limiterSuppliers = new ArrayList<>();
  private final List<BooleanSupplier> boosterSuppliers = new ArrayList<>();
  private GlobalChannelTrafficShapingHandler globalChannelTrafficShapingHandler;
  private final long baseRate;
  private final int signalIdleThreshold;
  private double currentFactor = 1.0;
  private static final double MIN_FACTOR = 0.2;
  private static final double MAX_FACTOR = 2.0;
  private final boolean isWriteThrottler;
  private int signalIdleCount;

  public AdaptiveBlobTransferTrafficThrottler(int singleIdleThreshold, long baseRate, boolean isWriteThrottler) {
    this.throttlerName = (isWriteThrottler ? "Write" : "Read") + THROTTLER_NAME_SUFFIX;
    this.baseRate = baseRate;
    this.signalIdleThreshold = singleIdleThreshold;
    this.isWriteThrottler = isWriteThrottler;
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
    if (globalChannelTrafficShapingHandler == null) {
      LOGGER.info("Related traffic shaping handler does not exist, will not update the throttling number");
      return;
    }
    boolean isSignalIdle = true;
    boolean hasLimitedRate = false;
    for (BooleanSupplier supplier: limiterSuppliers) {
      if (supplier.getAsBoolean()) {
        hasLimitedRate = true;
        isSignalIdle = false;
        signalIdleCount = 0;
        if (currentFactor > MIN_FACTOR) {
          currentFactor -= 0.2;
          updateThrottlerNumber();
          LOGGER.info(
              "Found limiter signal for {}, adjusting throttler factor to: {} with throttle rate: {}",
              throttlerName,
              currentFactor,
              currentFactor * baseRate);
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
        if (currentFactor < MAX_FACTOR) {
          currentFactor -= 0.2;
          updateThrottlerNumber();
          LOGGER.info(
              "Found booster signal for {}, adjusting throttler factor to: {} with throttle rate: {}",
              throttlerName,
              currentFactor,
              currentFactor * baseRate);
        }
      }
    }
    if (isSignalIdle) {
      signalIdleCount += 1;
      if (signalIdleCount == signalIdleThreshold) {
        if (currentFactor < MAX_FACTOR) {
          currentFactor += 0.2;
          updateThrottlerNumber();
          LOGGER.info(
              "Reach max signal idle count, adjusting throttler factor to: {} with throttle rate: {}",
              currentFactor,
              currentFactor * baseRate);
        }
        signalIdleCount = 0;
      }
    }
  }

  @Override
  public long getCurrentThrottlerRate() {
    return 0;
  }

  @Override
  public String getThrottlerName() {
    return null;
  }

  public void setGlobalChannelTrafficShapingHandler(
      GlobalChannelTrafficShapingHandler globalChannelTrafficShapingHandler) {
    this.globalChannelTrafficShapingHandler = globalChannelTrafficShapingHandler;
  }

  void updateThrottlerNumber() {
    if (isWriteThrottler) {
      globalChannelTrafficShapingHandler.setWriteLimit((long) currentFactor * baseRate);
    } else {
      globalChannelTrafficShapingHandler.setReadLimit((long) currentFactor * baseRate);
    }
  }
}
