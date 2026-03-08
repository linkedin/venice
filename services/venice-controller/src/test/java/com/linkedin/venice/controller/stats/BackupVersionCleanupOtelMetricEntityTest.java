package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.StoreBackupVersionCleanupServiceStats.BackupVersionCleanupOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class BackupVersionCleanupOtelMetricEntityTest {
  private static Map<BackupVersionCleanupOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<BackupVersionCleanupOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        BackupVersionCleanupOtelMetricEntity.BACKUP_VERSION_CLEANUP_MISMATCH_COUNT,
        new MetricEntityExpectation(
            "backup_version_cleanup_service.version_mismatch_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of backup version cleanup version mismatches",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(BackupVersionCleanupOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }
}
