package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.DeferredVersionSwapStats.DeferredVersionSwapTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class DeferredVersionSwapTehutiMetricNameEnumTest {
  private static Map<DeferredVersionSwapTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<DeferredVersionSwapTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_ERROR, "deferred_version_swap_error");
    map.put(DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_THROWABLE, "deferred_version_swap_throwable");
    map.put(
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_FAILED_ROLL_FORWARD,
        "deferred_version_swap_failed_roll_forward");
    map.put(
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_STALLED_VERSION_SWAP,
        "deferred_version_swap_stalled_version_swap");
    map.put(
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_PARENT_CHILD_STATUS_MISMATCH,
        "deferred_version_swap_parent_child_status_mismatch");
    map.put(
        DeferredVersionSwapTehutiMetricNameEnum.DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH,
        "deferred_version_swap_child_status_mismatch");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(DeferredVersionSwapTehutiMetricNameEnum.class, expectedMetricNames())
        .assertAll();
  }
}
