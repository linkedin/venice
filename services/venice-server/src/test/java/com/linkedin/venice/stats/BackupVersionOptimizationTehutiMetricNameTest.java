package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.TehutiMetricNameEnumTestFixture;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class BackupVersionOptimizationTehutiMetricNameTest {
  @Test
  public void testTehutiMetricNames() {
    new TehutiMetricNameEnumTestFixture<>(
        BackupVersionOptimizationServiceStats.TehutiMetricName.class,
        expectedMetricNames()).assertAll();
  }

  private static Map<BackupVersionOptimizationServiceStats.TehutiMetricName, String> expectedMetricNames() {
    Map<BackupVersionOptimizationServiceStats.TehutiMetricName, String> map = new HashMap<>();
    map.put(
        BackupVersionOptimizationServiceStats.TehutiMetricName.BACKUP_VERSION_DATABASE_OPTIMIZATION,
        "backup_version_database_optimization");
    map.put(
        BackupVersionOptimizationServiceStats.TehutiMetricName.BACKUP_VERSION_DATA_OPTIMIZATION_ERROR,
        "backup_version_data_optimization_error");
    return map;
  }
}
