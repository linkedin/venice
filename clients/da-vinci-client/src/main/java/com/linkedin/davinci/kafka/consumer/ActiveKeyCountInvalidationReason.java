package com.linkedin.davinci.kafka.consumer;

/**
 * Reasons for invalidating a partition's active-key-count tracking. Each value carries a message
 * template (some with a {@code %d} placeholder for runtime detail like the offending signal value
 * or header length) that becomes the prefix of the operator-visible ERROR log emitted by
 * {@link StoreIngestionTask#invalidateActiveKeyCount}.
 */
public enum ActiveKeyCountInvalidationReason {
  /** Follower received {@code kcs=-1} but its count was already zero (count drifted). */
  FOLLOWER_DECREMENT_UNDERFLOW("Decrement underflow on follower from kcs=-1"),
  /** Follower received {@code kcs=0} (leader-propagated invalidation signal). */
  LEADER_PROPAGATED_INVALIDATION("Leader propagated invalidation signal"),
  /** Follower received a single-byte {@code kcs} value outside the {-1, 0, +1} contract. */
  CORRUPT_KCS_SIGNAL_VALUE("Unexpected kcs signal value %d"),
  /** Follower received a multi-byte {@code kcs} header (corrupt or future producer). */
  CORRUPT_MULTI_BYTE_KCS_SIGNAL("Unexpected multi-byte kcs signal (length=%d)"),
  /** Leader detected an underflow during DCR (count was zero but a delete was processed). */
  LEADER_DCR_UNDERFLOW("Decrement underflow on leader during DCR"),
  /**
   * Leader's {@link com.linkedin.davinci.store.StorageEngine#keyExists} call (a RocksDB
   * value-column-family lookup used to determine whether a key currently has a live value) threw.
   * The transient I/O failure must not stop ingestion or leave the active count in a wrong state —
   * invalidate so we stop publishing a stale value.
   */
  KEY_EXISTS_FAILURE("RocksDB value column family lookup failed");

  private final String messageTemplate;

  ActiveKeyCountInvalidationReason(String messageTemplate) {
    this.messageTemplate = messageTemplate;
  }

  String getMessage() {
    return messageTemplate;
  }

  String getMessage(int detail) {
    return String.format(messageTemplate, detail);
  }
}
