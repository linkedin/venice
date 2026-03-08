package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.StoreBackupVersionCleanupServiceStats.BackupVersionCleanupOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class BackupVersionCleanupOtelMetricEntityTest
    extends AbstractModuleMetricEntityTest<BackupVersionCleanupOtelMetricEntity> {
  public BackupVersionCleanupOtelMetricEntityTest() {
    super(BackupVersionCleanupOtelMetricEntity.class);
  }

  @Override
  protected Map<BackupVersionCleanupOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
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
}
