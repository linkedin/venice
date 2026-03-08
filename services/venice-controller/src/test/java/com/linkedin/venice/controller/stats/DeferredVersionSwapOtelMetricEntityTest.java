package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.DeferredVersionSwapStats.DeferredVersionSwapOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class DeferredVersionSwapOtelMetricEntityTest
    extends AbstractModuleMetricEntityTest<DeferredVersionSwapOtelMetricEntity> {
  public DeferredVersionSwapOtelMetricEntityTest() {
    super(DeferredVersionSwapOtelMetricEntity.class);
  }

  @Override
  protected Map<DeferredVersionSwapOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<DeferredVersionSwapOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PROCESSING_ERROR_COUNT,
        new MetricEntityExpectation(
            "deferred_version_swap.processing_error_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of unexpected failures in the deferred version swap processing loop",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_ROLL_FORWARD_FAILURE_COUNT,
        new MetricEntityExpectation(
            "deferred_version_swap.roll_forward.failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of deferred version swap roll forward failures",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    map.put(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_STALLED_COUNT,
        new MetricEntityExpectation(
            "deferred_version_swap.stalled_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Count of stalled deferred version swaps across all clusters",
            Collections.emptySet()));
    map.put(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_PARENT_STATUS_MISMATCH_COUNT,
        new MetricEntityExpectation(
            "deferred_version_swap.parent_status_mismatch_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of deferred version swap parent-child status mismatches",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    map.put(
        DeferredVersionSwapOtelMetricEntity.DEFERRED_VERSION_SWAP_CHILD_STATUS_MISMATCH_COUNT,
        new MetricEntityExpectation(
            "deferred_version_swap.child_status_mismatch_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of deferred version swap child status mismatches",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    return map;
  }
}
