package com.linkedin.venice.stats.dimensions;

import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class AdminMessageProcessingComponentTest {
  @Test
  public void testDimensionInterface() {
    Map<AdminMessageProcessingComponent, String> expectedValues =
        CollectionUtils.<AdminMessageProcessingComponent, String>mapBuilder()
            .put(AdminMessageProcessingComponent.RETIRE_OLD_VERSIONS, "retire_old_versions")
            .put(AdminMessageProcessingComponent.RESOURCE_ASSIGNMENT_WAIT, "resource_assignment_wait")
            .put(AdminMessageProcessingComponent.FAILURE_HANDLING, "failure_handling")
            .put(AdminMessageProcessingComponent.EXISTING_VERSION_HANDLING, "existing_version_handling")
            .put(AdminMessageProcessingComponent.START_OF_PUSH, "start_of_push")
            .put(AdminMessageProcessingComponent.BATCH_TOPIC_CREATION, "batch_topic_creation")
            .put(AdminMessageProcessingComponent.HELIX_RESOURCE_CREATION, "helix_resource_creation")
            .build();
    new VeniceDimensionTestFixture<>(
        AdminMessageProcessingComponent.class,
        VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_PROCESSING_COMPONENT,
        expectedValues).assertAll();
  }
}
