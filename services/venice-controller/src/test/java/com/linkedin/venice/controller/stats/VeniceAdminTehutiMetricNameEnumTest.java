package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.VeniceAdminStats.VeniceAdminTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceAdminTehutiMetricNameEnumTest {
  private static Map<VeniceAdminTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<VeniceAdminTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(
        VeniceAdminTehutiMetricNameEnum.UNEXPECTED_TOPIC_ABSENCE_DURING_INCREMENTAL_PUSH_COUNT,
        "unexpected_topic_absence_during_incremental_push_count");
    map.put(
        VeniceAdminTehutiMetricNameEnum.SUCCESSFULLY_STARTED_USER_BATCH_PUSH_PARENT_ADMIN_COUNT,
        "successfully_started_user_batch_push_parent_admin_count");
    map.put(
        VeniceAdminTehutiMetricNameEnum.SUCCESSFUL_STARTED_USER_INCREMENTAL_PUSH_PARENT_ADMIN_COUNT,
        "successful_started_user_incremental_push_parent_admin_count");
    map.put(
        VeniceAdminTehutiMetricNameEnum.FAILED_SERIALIZING_ADMIN_OPERATION_MESSAGE_COUNT,
        "failed_serializing_admin_operation_message_count");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(VeniceAdminTehutiMetricNameEnum.class, expectedMetricNames()).assertAll();
  }
}
