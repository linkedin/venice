package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.StoreBackupVersionCleanupServiceStats.BackupVersionCleanupTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class BackupVersionCleanupTehutiMetricNameEnumTest {
  private static Map<BackupVersionCleanupTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<BackupVersionCleanupTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(
        BackupVersionCleanupTehutiMetricNameEnum.BACKUP_VERSION_CLEANUP_VERSION_MISMATCH,
        "backup_version_cleanup_version_mismatch");
    map.put(BackupVersionCleanupTehutiMetricNameEnum.ROLLED_BACK_VERSION_DELETED, "rolled_back_version_deleted");
    map.put(
        BackupVersionCleanupTehutiMetricNameEnum.ROLLED_BACK_VERSION_DELETE_ERROR,
        "rolled_back_version_delete_error");
    return map;
  }

  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(BackupVersionCleanupTehutiMetricNameEnum.class, expectedMetricNames())
        .assertAll();
  }
}
