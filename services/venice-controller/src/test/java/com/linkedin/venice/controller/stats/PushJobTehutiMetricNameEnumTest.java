package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.PushJobStatusStats.PushJobTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class PushJobTehutiMetricNameEnumTest {
  private static Map<PushJobTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<PushJobTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_SUCCESS, "batch_push_job_success");
    map.put(PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_USER_ERROR, "batch_push_job_failed_user_error");
    map.put(PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_NON_USER_ERROR, "batch_push_job_failed_non_user_error");
    map.put(PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_SUCCESS, "incremental_push_job_success");
    map.put(
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_USER_ERROR,
        "incremental_push_job_failed_user_error");
    map.put(
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_NON_USER_ERROR,
        "incremental_push_job_failed_non_user_error");
    map.put(PushJobTehutiMetricNameEnum.PUSH_JOB_EXTERNAL_STORAGE_WRITE_TIME, "push_job_external_storage_write_time");
    map.put(PushJobTehutiMetricNameEnum.PUSH_JOB_VENICE_WRITE_TIME, "push_job_venice_write_time");
    map.put(
        PushJobTehutiMetricNameEnum.PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE,
        "push_job_external_storage_write_failure");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(PushJobTehutiMetricNameEnum.class, expectedMetricNames()).assertAll();
  }
}
