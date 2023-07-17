package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.BitSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class RedundantExceptionFilter {
  public static final int DEFAULT_BITSET_SIZE = 8 * 1024 * 1024 * 16; // 16MB
  public static final long DEFAULT_NO_REDUNDANT_EXCEPTION_DURATION_MS = TimeUnit.SECONDS.toMillis(60); // 60s

  private static RedundantExceptionFilter singleton;

  private final int bitSetSize;
  private final ScheduledExecutorService cleanerExecutor = Executors.newScheduledThreadPool(1);

  private BitSet activeBitset;
  private BitSet oldBitSet;

  public RedundantExceptionFilter(int bitSetSize, long noRedundantExceptionDurationMs) {
    this.bitSetSize = bitSetSize;
    activeBitset = new BitSet(bitSetSize);
    oldBitSet = new BitSet(bitSetSize);
    cleanerExecutor.scheduleAtFixedRate(
        this::clearBitSet,
        noRedundantExceptionDurationMs,
        noRedundantExceptionDurationMs,
        TimeUnit.MILLISECONDS);
  }

  public synchronized static RedundantExceptionFilter getRedundantExceptionFilter() {
    if (singleton == null) {
      singleton = new RedundantExceptionFilter(DEFAULT_BITSET_SIZE, DEFAULT_NO_REDUNDANT_EXCEPTION_DURATION_MS);
    }
    return singleton;
  }

  public boolean isRedundantException(String exceptionMessage) {
    if (exceptionMessage == null) {
      return true;
    }
    int index = getIndex(exceptionMessage);
    return isRedundant(index);
  }

  public boolean isRedundantException(String storeName, Throwable e) {
    // By default use exception's class as the type. For VeniceException and RouterException use http status code
    // instead.
    String exceptionType = e.getClass().getName();
    if (e instanceof VeniceException) {
      exceptionType = String.valueOf(((VeniceException) e).getHttpStatusCode());
    }
    int index = getIndex(storeName + exceptionType);
    return isRedundant(index);
  }

  public boolean isRedundantException(String storeName, String exceptionType) {
    int index = getIndex(storeName + exceptionType);
    return isRedundant(index);
  }

  public final void clearBitSet() {
    synchronized (this) {
      // Swap bit sets so we are not blocked by clear operation.
      BitSet temp = oldBitSet;
      oldBitSet = activeBitset;
      activeBitset = temp;
    }
    oldBitSet.clear();
  }

  public void shutdown() {
    cleanerExecutor.shutdownNow();
  }

  private int getIndex(String key) {
    return Math.abs((key).hashCode() % bitSetSize);
  }

  private boolean isRedundant(int index) {
    if (!activeBitset.get(index)) {
      // It's possible that we found the bit was not set, then activeBitset is changed, and we set the bit in the new
      // set. But it doesn't matter.
      activeBitset.set(index);
      return false;
    }
    return true;
  }
}
