package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.PushJobStatusStats.PushJobTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class PushJobTehutiMetricNameEnumTest extends AbstractTehutiMetricNameEnumTest<PushJobTehutiMetricNameEnum> {
  public PushJobTehutiMetricNameEnumTest() {
    super(PushJobTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<PushJobTehutiMetricNameEnum, String> expectedMetricNames() {
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
    return map;
  }
}
