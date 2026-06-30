package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.stats.dimensions.VeniceDimensionTestFixture;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * Validates {@link ActiveKeyCountInvalidationReason} as an OTel dimension on
 * {@link com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity#ACTIVE_KEY_COUNT_INVALIDATION}:
 * enum value count, dimension name parity across all instances, and per-instance dimension value strings.
 * Uses raw hardcoded strings on purpose so accidental enum renames are caught.
 */
public class ActiveKeyCountInvalidationReasonTest {
  @Test
  public void testDimensionInterface() {
    Map<ActiveKeyCountInvalidationReason, String> expectedValues =
        CollectionUtils.<ActiveKeyCountInvalidationReason, String>mapBuilder()
            .put(ActiveKeyCountInvalidationReason.FOLLOWER_DECREMENT_UNDERFLOW, "follower_decrement_underflow")
            .put(ActiveKeyCountInvalidationReason.LEADER_PROPAGATED_INVALIDATION, "leader_propagated_invalidation")
            .put(
                ActiveKeyCountInvalidationReason.CORRUPT_KEY_COUNT_SIGNAL_HEADER_VALUE,
                "corrupt_key_count_signal_header_value")
            .put(
                ActiveKeyCountInvalidationReason.CORRUPT_KEY_COUNT_SIGNAL_HEADER_LENGTH,
                "corrupt_key_count_signal_header_length")
            .put(ActiveKeyCountInvalidationReason.LEADER_DCR_UNDERFLOW, "leader_dcr_underflow")
            .put(ActiveKeyCountInvalidationReason.KEY_EXISTS_FAILURE, "key_exists_failure")
            .put(
                ActiveKeyCountInvalidationReason.CORRUPT_LEADER_KEY_COUNT_HEADER_LENGTH,
                "corrupt_leader_key_count_header_length")
            .build();
    new VeniceDimensionTestFixture<>(
        ActiveKeyCountInvalidationReason.class,
        VeniceMetricsDimensions.VENICE_ACTIVE_KEY_COUNT_INVALIDATION_REASON,
        expectedValues).assertAll();
  }
}
