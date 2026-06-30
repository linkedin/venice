package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ACTIVE_KEY_COUNT_INVALIDATION_REASON;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;


/**
 * Reasons for invalidating a partition's active-key-count tracking. Each value carries a message
 * template (some with a {@code %d} placeholder for runtime detail like the offending signal value
 * or header length) that becomes the prefix of the operator-visible ERROR log emitted by
 * {@link StoreIngestionTask#invalidateActiveKeyCount}.
 *
 * <p>Also serves as an OTel dimension on
 * {@link com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#ACTIVE_KEY_COUNT_INVALIDATION}.
 * The Tehuti sensor remains a flat total.
 */
public enum ActiveKeyCountInvalidationReason implements VeniceDimensionInterface {
  /** Follower received {@code kcs=-1} but its count was already zero (count drifted). */
  FOLLOWER_DECREMENT_UNDERFLOW("Decrement underflow on follower from kcs=-1"),
  /** Follower received {@code kcs=0} (leader-propagated invalidation signal). */
  LEADER_PROPAGATED_INVALIDATION("Leader propagated invalidation signal"),
  /** Follower received a single-byte {@code kcs} value outside the {-1, 0, +1} contract. */
  CORRUPT_KEY_COUNT_SIGNAL_HEADER_VALUE("Unexpected kcs signal value %d"),
  /** Follower received a {@code kcs} header with the wrong byte length (expected 1; corrupt or future producer). */
  CORRUPT_KEY_COUNT_SIGNAL_HEADER_LENGTH("Unexpected kcs header length=%d"),
  /** Leader detected an underflow during DCR (count was zero but a delete was processed). */
  LEADER_DCR_UNDERFLOW("Decrement underflow on leader during DCR"),
  /**
   * Leader's {@link com.linkedin.davinci.store.StorageEngine#keyExists} call (a RocksDB
   * value-column-family lookup used to determine whether a key currently has a live value) threw.
   * The transient I/O failure must not stop ingestion or leave the active count in a wrong state —
   * invalidate so we stop publishing a stale value.
   */
  KEY_EXISTS_FAILURE("RocksDB value column family lookup failed"),
  /** Follower received an {@code lkc} header with the wrong byte length (expected 8; corrupt or future producer). */
  CORRUPT_LEADER_KEY_COUNT_HEADER_LENGTH("Unexpected lkc header length=%d");

  private final String messageTemplate;
  private final boolean templateWithExtraData;

  ActiveKeyCountInvalidationReason(String messageTemplate) {
    this.messageTemplate = messageTemplate;
    this.templateWithExtraData = messageTemplate.contains("%d");
  }

  String getMessage() {
    if (templateWithExtraData) {
      throw new IllegalStateException(
          "Reason " + name() + " carries a '%d' placeholder; caller must use getMessage(int detail).");
    }
    return messageTemplate;
  }

  String getMessage(int detail) {
    if (!templateWithExtraData) {
      throw new IllegalStateException(
          "Reason " + name() + " has no '%d' placeholder; caller must use the no-arg getMessage().");
    }
    return String.format(messageTemplate, detail);
  }

  /**
   * All instances of this enum share the same dimension name.
   * Refer to {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VENICE_ACTIVE_KEY_COUNT_INVALIDATION_REASON;
  }
}
