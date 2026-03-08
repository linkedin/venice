package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.SystemStoreHealthCheckStats.SystemStoreHealthCheckTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class SystemStoreHealthCheckTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<SystemStoreHealthCheckTehutiMetricNameEnum> {
  public SystemStoreHealthCheckTehutiMetricNameEnumTest() {
    super(SystemStoreHealthCheckTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<SystemStoreHealthCheckTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<SystemStoreHealthCheckTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(SystemStoreHealthCheckTehutiMetricNameEnum.BAD_META_SYSTEM_STORE_COUNT, "bad_meta_system_store_count");
    map.put(
        SystemStoreHealthCheckTehutiMetricNameEnum.BAD_PUSH_STATUS_SYSTEM_STORE_COUNT,
        "bad_push_status_system_store_count");
    map.put(
        SystemStoreHealthCheckTehutiMetricNameEnum.NOT_REPAIRABLE_SYSTEM_STORE_COUNT,
        "not_repairable_system_store_count");
    return map;
  }
}
