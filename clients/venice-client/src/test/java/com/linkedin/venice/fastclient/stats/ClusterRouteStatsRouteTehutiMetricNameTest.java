package com.linkedin.venice.fastclient.stats;

import com.linkedin.venice.fastclient.stats.ClusterRouteStats.RouteTehutiMetricName;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class ClusterRouteStatsRouteTehutiMetricNameTest
    extends AbstractTehutiMetricNameEnumTest<RouteTehutiMetricName> {
  public ClusterRouteStatsRouteTehutiMetricNameTest() {
    super(RouteTehutiMetricName.class);
  }

  @Override
  protected Map<RouteTehutiMetricName, String> expectedMetricNames() {
    Map<RouteTehutiMetricName, String> map = new HashMap<>();
    map.put(RouteTehutiMetricName.HEALTHY_REQUEST_COUNT, "healthy_request_count");
    map.put(RouteTehutiMetricName.QUOTA_EXCEEDED_REQUEST_COUNT, "quota_exceeded_request_count");
    map.put(RouteTehutiMetricName.INTERNAL_SERVER_ERROR_REQUEST_COUNT, "internal_server_error_request_count");
    map.put(RouteTehutiMetricName.LEAKED_REQUEST_COUNT, "leaked_request_count");
    map.put(RouteTehutiMetricName.SERVICE_UNAVAILABLE_REQUEST_COUNT, "service_unavailable_request_count");
    map.put(RouteTehutiMetricName.OTHER_ERROR_REQUEST_COUNT, "other_error_request_count");
    map.put(RouteTehutiMetricName.RESPONSE_WAITING_TIME, "response_waiting_time");
    map.put(RouteTehutiMetricName.PENDING_REQUEST_COUNT, "pending_request_count");
    map.put(RouteTehutiMetricName.REJECTION_RATIO, "rejection_ratio");
    return map;
  }
}
