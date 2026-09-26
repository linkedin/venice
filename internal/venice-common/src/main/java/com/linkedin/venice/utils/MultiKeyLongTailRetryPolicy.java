package com.linkedin.venice.utils;

import java.util.Collections;
import java.util.NavigableMap;


/**
 * An immutable, validated server policy shared by compute and batch-get. Parse once per metadata refresh,
 * then capture one delay at request entry. A null policy at the metadata boundary means local fallback.
 */
public final class MultiKeyLongTailRetryPolicy {
  private final NavigableMap<Integer, Integer> thresholdsInMs;

  private MultiKeyLongTailRetryPolicy(String ranges) {
    thresholdsInMs =
        Collections.unmodifiableNavigableMap(BatchGetConfigUtils.parseServerMultiKeyRetryThresholds(ranges));
  }

  public static MultiKeyLongTailRetryPolicy parse(String ranges) {
    return new MultiKeyLongTailRetryPolicy(ranges);
  }

  public int getRetryThresholdInMicroSeconds(int keyCount) {
    if (keyCount <= 0) {
      throw new IllegalArgumentException("Retry policy requires a positive key count");
    }
    return Math.multiplyExact(thresholdsInMs.floorEntry(keyCount).getValue(), 1000);
  }
}
