package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.BackupVersionOptimizationOtelMetricEntity.REOPEN_COUNT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_OPERATION_OUTCOME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class BackupVersionOptimizationOtelMetricEntityTest {
  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(BackupVersionOptimizationOtelMetricEntity.class, expectedDefinitions())
        .assertAll();
  }

  private static Map<BackupVersionOptimizationOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<BackupVersionOptimizationOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        REOPEN_COUNT,
        new MetricEntityExpectation(
            "version.backup.optimization.reopen_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of backup version storage partition reopens by outcome (success or fail)",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_OPERATION_OUTCOME)));
    return map;
  }
}
