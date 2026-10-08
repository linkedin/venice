package com.linkedin.venice.utils;

import java.util.Collections;
import java.util.NavigableMap;


/**
 * Immutable, validated server retry thresholds shared by compute and batch-get. Parse once per metadata refresh,
 * then capture one delay at request entry. Null thresholds at the metadata boundary mean local fallback.
 */
public final class MultiKeyLongTailRetryThresholds {
  private final NavigableMap<Integer, Integer> thresholdsInMs;

  private MultiKeyLongTailRetryThresholds(String ranges) {
    thresholdsInMs =
        Collections.unmodifiableNavigableMap(BatchGetConfigUtils.parseServerMultiKeyRetryThresholds(ranges));
  }

  public static MultiKeyLongTailRetryThresholds parse(String ranges) {
    return new MultiKeyLongTailRetryThresholds(ranges);
  }

  public int getRetryThresholdInMicroSeconds(int keyCount) {
    if (keyCount <= 0) {
      throw new IllegalArgumentException("Retry thresholds require a positive key count");
    }
    return Math.multiplyExact(thresholdsInMs.floorEntry(keyCount).getValue(), 1000);
  }
}
