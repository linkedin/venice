package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.STORE_REPUSH_TRIGGER_SOURCE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.LogCompactionStats.LogCompactionOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class LogCompactionOtelMetricEntityTest extends AbstractModuleMetricEntityTest<LogCompactionOtelMetricEntity> {
  public LogCompactionOtelMetricEntityTest() {
    super(LogCompactionOtelMetricEntity.class);
  }

  @Override
  protected Map<LogCompactionOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<LogCompactionOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        LogCompactionOtelMetricEntity.STORE_REPUSH_CALL_COUNT,
        new MetricEntityExpectation(
            "store.repush.call_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of all requests to repush a store",
            setOf(
                VENICE_STORE_NAME,
                VENICE_RESPONSE_STATUS_CODE_CATEGORY,
                VENICE_CLUSTER_NAME,
                STORE_REPUSH_TRIGGER_SOURCE)));
    map.put(
        LogCompactionOtelMetricEntity.STORE_COMPACTION_NOMINATED_COUNT,
        new MetricEntityExpectation(
            "store.compaction.nominated_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of stores nominated for scheduled compaction",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        LogCompactionOtelMetricEntity.STORE_COMPACTION_ELIGIBLE_STATE,
        new MetricEntityExpectation(
            "store.compaction.eligible_state",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Track the state from the time a store is nominated for compaction to the time the repush is completed",
            setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME)));
    map.put(
        LogCompactionOtelMetricEntity.STORE_COMPACTION_TRIGGERED_COUNT,
        new MetricEntityExpectation(
            "store.compaction.triggered_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of log compaction repush triggered for a store after it becomes eligible",
            setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY, VENICE_CLUSTER_NAME)));
    return map;
  }
}
