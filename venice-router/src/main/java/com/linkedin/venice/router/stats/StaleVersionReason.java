package com.linkedin.venice.router.stats;

/**
 * This enum is used to denote why a particular store version was found to be stale
 */
public enum StaleVersionReason {
  OFFLINE_PARTITIONS, DICTIONARY_NOT_DOWNLOADED
}
