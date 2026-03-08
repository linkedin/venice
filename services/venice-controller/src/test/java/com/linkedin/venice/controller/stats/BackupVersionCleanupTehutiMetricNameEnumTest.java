package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.StoreBackupVersionCleanupServiceStats.BackupVersionCleanupTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class BackupVersionCleanupTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<BackupVersionCleanupTehutiMetricNameEnum> {
  public BackupVersionCleanupTehutiMetricNameEnumTest() {
    super(BackupVersionCleanupTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<BackupVersionCleanupTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<BackupVersionCleanupTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(
        BackupVersionCleanupTehutiMetricNameEnum.BACKUP_VERSION_CLEANUP_VERSION_MISMATCH,
        "backup_version_cleanup_version_mismatch");
    return map;
  }
}
