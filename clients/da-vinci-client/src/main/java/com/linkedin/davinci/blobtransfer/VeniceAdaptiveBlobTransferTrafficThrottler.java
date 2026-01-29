package com.linkedin.davinci.blobtransfer;

import com.linkedin.venice.throttle.VeniceAdaptiveThrottler;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Adaptive throttling wrapper for {@link io.netty.handler.traffic.GlobalChannelTrafficShapingHandler}
 * that dynamically tunes global read or write limits for blob transfer based on registered boolean signals.
 * It controls either global read or write throughput for the blob transfer behavior based on results of the registered
 * limiter and booster signals.
 *
 * The heuristic behavior rules are defined as:
 * (1) If any limiter signal found, it will decrease by 20%. (min 20% of the base rate).
 * (2) If no limiter signal found, but there is booster signal found, it will increase by 20%. (max 200% of the base rate).
 * (3) If none of the above rule applies, it will increase idle count, if idle count is greater than the pre-defined
 * threshold, it will try to increase by 20% (still max 200% of the base rate).
 *
 * Base rate is initially set to {@link com.linkedin.venice.ConfigKeys#BLOB_TRANSFER_SERVICE_WRITE_LIMIT_BYTES_PER_SEC}
 * or {@link com.linkedin.venice.ConfigKeys#BLOB_TRANSFER_CLIENT_READ_LIMIT_BYTES_PER_SEC} and will be adjusted based on
 * input signals dynamically.
 */
public class VeniceAdaptiveBlobTransferTrafficThrottler implements VeniceAdaptiveThrottler {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAdaptiveBlobTransferTrafficThrottler.class);
  private static final String THROTTLER_NAME_SUFFIX = "TrafficBlobTransferThrottler";
  private final String throttlerName;
  private final List<BooleanSupplier> limiterSuppliers = new ArrayList<>();
  private final List<BooleanSupplier> boosterSuppliers = new ArrayList<>();
  private final AtomicReference<GlobalChannelTrafficShapingHandler> globalChannelTrafficShapingHandler =
      new AtomicReference<>();
  private final long baseRate;
  private final int signalIdleThreshold;
  private int currentFactor = 100;
  private static final int MIN_FACTOR = 20;
  private static final int MAX_FACTOR = 200;
  private final int rateUpdatePercentage;
  private final boolean isWriteThrottler;
  private int signalIdleCount;

  public VeniceAdaptiveBlobTransferTrafficThrottler(
      int singleIdleThreshold,
      long baseRate,
      int rateUpdatePercentage,
      boolean isWriteThrottler) {
    this.throttlerName = (isWriteThrottler ? "Write" : "Read") + THROTTLER_NAME_SUFFIX;
    this.baseRate = baseRate;
    this.signalIdleThreshold = singleIdleThreshold;
    this.isWriteThrottler = isWriteThrottler;
    this.rateUpdatePercentage = rateUpdatePercentage;
    LOGGER.info(
        "Created adaptive throttler with name: {}, base rate: {}, change delta: {}",
        throttlerName,
        baseRate,
        rateUpdatePercentage);
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
    if (globalChannelTrafficShapingHandler.get() == null) {
      LOGGER.info("Related traffic shaping handler does not exist, will not update the throttling number");
      return;
    }
    boolean isSignalIdle = true;
    boolean hasLimiterRate = false;
    for (BooleanSupplier supplier: limiterSuppliers) {
      if (supplier.getAsBoolean()) {
        hasLimiterRate = true;
        isSignalIdle = false;
        signalIdleCount = 0;
      }
    }
    // If any limiter signal is true do not booster the throttler
    if (hasLimiterRate) {
      if (currentFactor > MIN_FACTOR) {
        currentFactor = Math.max(MIN_FACTOR, currentFactor - rateUpdatePercentage);
        updateThrottlerNumber();
        LOGGER.info(
            "Found limiter signal for: {}, adjusting throttler factor to: {} with throttle rate: {}",
            throttlerName,
            currentFactor,
            currentFactor * baseRate / 100);
      }
      return;
    }
    boolean hasBoosterRate = false;
    for (BooleanSupplier supplier: boosterSuppliers) {
      if (supplier.getAsBoolean()) {
        isSignalIdle = false;
        signalIdleCount = 0;
        hasBoosterRate = true;
      }
    }

    if (hasBoosterRate) {
      if (currentFactor < MAX_FACTOR) {
        currentFactor = Math.min(MAX_FACTOR, currentFactor + rateUpdatePercentage);
        updateThrottlerNumber();
        LOGGER.info(
            "Found booster signal for: {}, adjusting throttler factor to: {} with throttle rate: {}",
            throttlerName,
            currentFactor,
            currentFactor * baseRate);
      }
    }

    if (isSignalIdle) {
      signalIdleCount += 1;
      if (signalIdleCount == signalIdleThreshold) {
        if (currentFactor < MAX_FACTOR) {
          currentFactor = Math.min(MAX_FACTOR, currentFactor + rateUpdatePercentage);
          updateThrottlerNumber();
          LOGGER.info(
              "Reach max signal idle count: {} for: {}, adjusting throttler factor to: {} with throttle rate: {}",
              signalIdleThreshold,
              throttlerName,
              currentFactor,
              currentFactor * baseRate / 100);
        }
        signalIdleCount = 0;
      }
    }
  }

  @Override
  public long getCurrentThrottlerRate() {
    return (long) (currentFactor * baseRate / 100.0);
  }

  @Override
  public String getThrottlerName() {
    return throttlerName;
  }

  public void setGlobalChannelTrafficShapingHandler(
      GlobalChannelTrafficShapingHandler globalChannelTrafficShapingHandler) {
    if (!this.globalChannelTrafficShapingHandler.compareAndSet(null, globalChannelTrafficShapingHandler)) {
      throw new UnsupportedOperationException("Cannot update GlobalChannelTrafficShapingHandler once initialized.");
    }
  }

  void updateThrottlerNumber() {
    if (isWriteThrottler) {
      globalChannelTrafficShapingHandler.get().setWriteLimit((long) (currentFactor * baseRate / 100.0));
    } else {
      globalChannelTrafficShapingHandler.get().setReadLimit((long) (currentFactor * baseRate / 100.0));
    }
  }
}
