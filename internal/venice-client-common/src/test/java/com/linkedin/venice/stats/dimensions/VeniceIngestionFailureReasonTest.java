package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceIngestionFailureReasonTest {
  @Test
  public void testDimensionInterface() {
    Map<VeniceIngestionFailureReason, String> expectedValues =
        CollectionUtils.<VeniceIngestionFailureReason, String>mapBuilder()
            .put(VeniceIngestionFailureReason.TASK_KILLED, "task_killed")
            .put(VeniceIngestionFailureReason.CHECKSUM_VERIFICATION_FAILURE, "checksum_verification_failure")
            .put(VeniceIngestionFailureReason.SERVING_VERSION_BOOTSTRAP_TIMEOUT, "serving_version_bootstrap_timeout")
            .put(VeniceIngestionFailureReason.FUTURE_VERSION_PUSH_TIMEOUT, "future_version_push_timeout")
            .put(VeniceIngestionFailureReason.REMOTE_BROKER_UNREACHABLE, "remote_broker_unreachable")
            .put(VeniceIngestionFailureReason.GENERAL, "general")
            .build();
    new VeniceDimensionTestFixture<>(
        VeniceIngestionFailureReason.class,
        VeniceMetricsDimensions.VENICE_INGESTION_FAILURE_REASON,
        expectedValues).assertAll();
  }
}
