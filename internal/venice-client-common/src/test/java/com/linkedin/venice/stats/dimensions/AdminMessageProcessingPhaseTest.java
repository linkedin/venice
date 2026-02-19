package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class AdminMessageProcessingPhaseTest extends VeniceDimensionInterfaceTest<AdminMessageProcessingPhase> {
  protected AdminMessageProcessingPhaseTest() {
    super(AdminMessageProcessingPhase.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_PROCESSING_PHASE;
  }

  @Override
  protected Map<AdminMessageProcessingPhase, String> expectedDimensionValueMapping() {
    return CollectionUtils.<AdminMessageProcessingPhase, String>mapBuilder()
        .put(AdminMessageProcessingPhase.RETIRE_OLD_VERSIONS, "retire_old_versions")
        .put(AdminMessageProcessingPhase.RESOURCE_ASSIGNMENT_WAIT, "resource_assignment_wait")
        .put(AdminMessageProcessingPhase.FAILURE_HANDLING, "failure_handling")
        .put(AdminMessageProcessingPhase.EXISTING_VERSION_HANDLING, "existing_version_handling")
        .put(AdminMessageProcessingPhase.START_OF_PUSH, "start_of_push")
        .put(AdminMessageProcessingPhase.BATCH_TOPIC_CREATION, "batch_topic_creation")
        .put(AdminMessageProcessingPhase.HELIX_RESOURCE_CREATION, "helix_resource_creation")
        .build();
  }
}
