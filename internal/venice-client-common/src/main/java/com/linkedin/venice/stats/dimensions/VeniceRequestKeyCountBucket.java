package com.linkedin.venice.stats.dimensions;

/**
 * Coarse buckets for request key count, used as an OTel dimension on CallTime metrics so that
 * SLOs can distinguish latency across batch sizes without blowing up cardinality with raw counts.
 *
 * <p>{@link #KEYS_LE_0} captures recordings that carry a non-positive key count — in practice the
 * {@code -1} sentinel that server-side pre-parse errors emit when the request failed before its
 * key count could be parsed. Dashboards should filter this bucket out of SLO views since the
 * latency it represents is not tied to a real batch size.
 */
public enum VeniceRequestKeyCountBucket implements VeniceDimensionInterface {
  /** Key count {@code <= 0}: pre-parse error sentinel (e.g., server {@code requestKeyCount == -1}). */
  KEYS_LE_0,
  /** Single-key request (single-get, or a multiget / compute with exactly one key). */
  KEYS_EQ_1,
  /** Small batch: 2 to 150 keys inclusive. */
  KEYS_2_150,
  /** Medium batch: 151 to 500 keys inclusive. */
  KEYS_151_500,
  /** Medium-large batch: 501 to 1000 keys inclusive. */
  KEYS_501_1000,
  /** Large batch: 1001 to 2000 keys inclusive. */
  KEYS_1001_2000,
  /** Very large batch: 2001 to 5000 keys inclusive. */
  KEYS_2001_5000,
  /** Oversized batch: more than 5000 keys. */
  KEYS_GT_5000;

  /**
   * All instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.VENICE_REQUEST_KEY_COUNT_BUCKET;
  }

  /**
   * Single-get short-circuits in a single comparison. Multi-key requests fall through to a
   * balanced comparison tree. The non-positive sentinel falls through to the terminal return.
   */
  public static VeniceRequestKeyCountBucket fromKeyCount(int keyCount) {
    if (keyCount == 1) {
      return KEYS_EQ_1;
    } else if (keyCount > 1) {
      if (keyCount <= 500) {
        if (keyCount <= 150) {
          return KEYS_2_150;
        }
        return KEYS_151_500;
      } else if (keyCount <= 2000) {
        if (keyCount <= 1000) {
          return KEYS_501_1000;
        }
        return KEYS_1001_2000;
      } else if (keyCount <= 5000) {
        return KEYS_2001_5000;
      }
      return KEYS_GT_5000;
    }
    return KEYS_LE_0;
  }
}
