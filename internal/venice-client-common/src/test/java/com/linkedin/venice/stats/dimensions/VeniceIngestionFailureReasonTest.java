package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class VeniceIngestionFailureReasonTest extends VeniceDimensionInterfaceTest<VeniceIngestionFailureReason> {
  protected VeniceIngestionFailureReasonTest() {
    super(VeniceIngestionFailureReason.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_INGESTION_FAILURE_REASON;
  }

  @Override
  protected Map<VeniceIngestionFailureReason, String> expectedDimensionValueMapping() {
    return CollectionUtils.<VeniceIngestionFailureReason, String>mapBuilder()
        .put(VeniceIngestionFailureReason.TASK_KILLED, "task_killed")
        .put(VeniceIngestionFailureReason.CHECKSUM_VERIFICATION_FAILURE, "checksum_verification_failure")
        .put(VeniceIngestionFailureReason.SERVING_VERSION_BOOTSTRAP_TIMEOUT, "serving_version_bootstrap_timeout")
        .put(VeniceIngestionFailureReason.FUTURE_VERSION_PUSH_TIMEOUT, "future_version_push_timeout")
        .put(VeniceIngestionFailureReason.REMOTE_BROKER_UNREACHABLE, "remote_broker_unreachable")
        .put(VeniceIngestionFailureReason.GENERAL, "general")
        .build();
  }
}
