package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.AdminConsumptionStats.AdminConsumptionTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class AdminConsumptionTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<AdminConsumptionTehutiMetricNameEnum> {
  public AdminConsumptionTehutiMetricNameEnumTest() {
    super(AdminConsumptionTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<AdminConsumptionTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<AdminConsumptionTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(AdminConsumptionTehutiMetricNameEnum.FAILED_ADMIN_MESSAGES, "failed_admin_messages");
    map.put(AdminConsumptionTehutiMetricNameEnum.FAILED_RETRIABLE_ADMIN_MESSAGES, "failed_retriable_admin_messages");
    map.put(
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_DIV_ERROR_REPORT_COUNT,
        "admin_message_div_error_report_count");
    map.put(
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGES_WITH_FUTURE_PROTOCOL_VERSION_COUNT,
        "admin_messages_with_future_protocol_version_count");
    map.put(AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_MM_LATENCY_MS, "admin_message_mm_latency_ms");
    map.put(
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_DELEGATE_LATENCY_MS,
        "admin_message_delegate_latency_ms");
    map.put(
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_START_PROCESSING_LATENCY_MS,
        "admin_message_start_processing_latency_ms");
    map.put(
        AdminConsumptionTehutiMetricNameEnum.ADMIN_CONSUMPTION_CYCLE_DURATION_MS,
        "admin_consumption_cycle_duration_ms");
    map.put(AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_PROCESS_LATENCY_MS, "admin_message_process_latency_ms");
    map.put(
        AdminConsumptionTehutiMetricNameEnum.ADMIN_MESSAGE_ADD_VERSION_PROCESS_LATENCY_MS,
        "admin_message_add_version_process_latency_ms");
    map.put(AdminConsumptionTehutiMetricNameEnum.PENDING_ADMIN_MESSAGES_COUNT, "pending_admin_messages_count");
    map.put(
        AdminConsumptionTehutiMetricNameEnum.STORES_WITH_PENDING_ADMIN_MESSAGES_COUNT,
        "stores_with_pending_admin_messages_count");
    map.put(AdminConsumptionTehutiMetricNameEnum.ADMIN_CONSUMPTION_OFFSET_LAG, "admin_consumption_offset_lag");
    map.put(AdminConsumptionTehutiMetricNameEnum.MAX_ADMIN_CONSUMPTION_OFFSET_LAG, "max_admin_consumption_offset_lag");
    return map;
  }
}
