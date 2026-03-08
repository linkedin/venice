package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.ErrorPartitionStats.ErrorPartitionTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class ErrorPartitionTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<ErrorPartitionTehutiMetricNameEnum> {
  public ErrorPartitionTehutiMetricNameEnumTest() {
    super(ErrorPartitionTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<ErrorPartitionTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<ErrorPartitionTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(
        ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT,
        "current_version_error_partition_reset_attempt");
    map.put(
        ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RESET_ATTEMPT_ERRORED,
        "current_version_error_partition_reset_attempt_errored");
    map.put(
        ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_RECOVERED_FROM_RESET,
        "current_version_error_partition_recovered_from_reset");
    map.put(
        ErrorPartitionTehutiMetricNameEnum.CURRENT_VERSION_ERROR_PARTITION_UNRECOVERABLE_FROM_RESET,
        "current_version_error_partition_unrecoverable_from_reset");
    map.put(ErrorPartitionTehutiMetricNameEnum.ERROR_PARTITION_PROCESSING_ERROR, "error_partition_processing_error");
    map.put(ErrorPartitionTehutiMetricNameEnum.ERROR_PARTITION_PROCESSING_TIME, "error_partition_processing_time");
    return map;
  }
}
