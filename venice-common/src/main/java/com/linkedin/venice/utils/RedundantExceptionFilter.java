package com.linkedin.venice.utils;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


public class RedundantExceptionFilter {
  public static final int DEFAULT_BITSET_SIZE = 8 * 1024 * 1024 * 16; //16MB
  public static final long DEFAULT_NO_REDUNDANT_EXCEPTION_DURATION_MS = TimeUnit.SECONDS.toMillis(60); // 60s
  public static final Logger logger = Logger.getLogger(RedundantExceptionFilter.class);
  private static RedundantExceptionFilter singleton;
  private BitSet activeBitset;
  private BitSet oldBitSet;

  private final long noRedundantExceptionDurationMs;
  private final int bitSetSize;

  private final Thread cleanerThread;

  protected RedundantExceptionFilter(int bitSetSize, long noRedundantExceptionDurationMs) {
    this.noRedundantExceptionDurationMs = noRedundantExceptionDurationMs;
    this.bitSetSize = bitSetSize;
    activeBitset = new BitSet(bitSetSize);
    oldBitSet = new BitSet(bitSetSize);
    // Start a cleaner thread to clear bit sets every noRedundantExceptionDurationMs
    cleanerThread = new Thread(() -> {
      while (true) {
        try {
          Utils.sleep(this.noRedundantExceptionDurationMs);
          clearBitSet();
        } catch (Throwable e) {
          logger.error("Get an exception in cleaner thread.", e);
        }
      }
    });
    cleanerThread.start();
  }

  public synchronized static RedundantExceptionFilter getRedundantExceptionFilter(){
    return getRedundantExceptionFilter(DEFAULT_BITSET_SIZE, DEFAULT_NO_REDUNDANT_EXCEPTION_DURATION_MS);
  }

  public synchronized static RedundantExceptionFilter getDailyRedundantExceptioFilter(){
    // clean up the bitset every day for the use case do not need to clean up frequently.
    return getRedundantExceptionFilter(DEFAULT_BITSET_SIZE, Time.MS_PER_DAY);
  }

  public synchronized static RedundantExceptionFilter getRedundantExceptionFilter(int bitSetSize,
      long noRedundantExceptionDurationMs) {
    if (singleton == null) {
      singleton = new RedundantExceptionFilter(bitSetSize, noRedundantExceptionDurationMs);
    }
    return singleton;
  }

  private int getIndex(String key) {
    return Math.abs((key).hashCode() % bitSetSize);
  }


  public boolean isRedundantException(String exceptionMessage){
    int index = getIndex(exceptionMessage);
    return isRedundant(index);
  }

  public boolean isRedundantException(String storeName, Throwable e) {
    // By default use exception's class as the type. For VeniceException and RouterException use http status code instead.
    String exceptionType = e.getClass().getName();
    if (e instanceof RouterException) {
      exceptionType = String.valueOf(((RouterException) e).code());
    } else if (e instanceof VeniceException) {
      exceptionType = String.valueOf(((VeniceException) e).getHttpStatusCode());
    }
    int index = getIndex(storeName + exceptionType);
    return isRedundant(index);
  }

  private boolean isRedundant(int index){
    if (!activeBitset.get(index)) {
      // It's possible that we found the bit was not set, then activeBitset is changed, and we set the bit in the new
      // set. But it doesn't matter.
      activeBitset.set(index);
      return false;
    }
    return true;
  }

  public void clearBitSet() {
    synchronized (this) {
      // Swap bit sets so we are not blocked by clear operation.
      BitSet temp = oldBitSet;
      oldBitSet = activeBitset;
      activeBitset = temp;
    }
    oldBitSet.clear();
  }

}
