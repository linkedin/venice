package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class RouterHttpRequestStatsRouterTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<RouterHttpRequestStats.RouterTehutiMetricNameEnum> {
  public RouterHttpRequestStatsRouterTehutiMetricNameEnumTest() {
    super(RouterHttpRequestStats.RouterTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<RouterHttpRequestStats.RouterTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<RouterHttpRequestStats.RouterTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.HEALTHY_REQUEST, "healthy_request");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.UNHEALTHY_REQUEST, "unhealthy_request");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.TARDY_REQUEST, "tardy_request");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.THROTTLED_REQUEST, "throttled_request");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.BAD_REQUEST, "bad_request");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.HEALTHY_REQUEST_LATENCY, "healthy_request_latency");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.UNHEALTHY_REQUEST_LATENCY, "unhealthy_request_latency");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.TARDY_REQUEST_LATENCY, "tardy_request_latency");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.THROTTLED_REQUEST_LATENCY, "throttled_request_latency");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.ERROR_RETRY, "error_retry");
    map.put(
        RouterHttpRequestStats.RouterTehutiMetricNameEnum.ALLOWED_RETRY_REQUEST_COUNT,
        "allowed_retry_request_count");
    map.put(
        RouterHttpRequestStats.RouterTehutiMetricNameEnum.DISALLOWED_RETRY_REQUEST_COUNT,
        "disallowed_retry_request_count");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.RETRY_DELAY, "retry_delay");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.REQUEST_SIZE, "request_size");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.RESPONSE_SIZE, "response_size");
    map.put(RouterHttpRequestStats.RouterTehutiMetricNameEnum.KEY_SIZE_IN_BYTE, "key_size_in_byte");
    map.put(
        RouterHttpRequestStats.RouterTehutiMetricNameEnum.DELAY_CONSTRAINT_ABORTED_RETRY_REQUEST,
        "delay_constraint_aborted_retry_request");
    map.put(
        RouterHttpRequestStats.RouterTehutiMetricNameEnum.SLOW_ROUTE_ABORTED_RETRY_REQUEST,
        "slow_route_aborted_retry_request");
    map.put(
        RouterHttpRequestStats.RouterTehutiMetricNameEnum.RETRY_ROUTE_LIMIT_ABORTED_RETRY_REQUEST,
        "retry_route_limit_aborted_retry_request");
    map.put(
        RouterHttpRequestStats.RouterTehutiMetricNameEnum.NO_AVAILABLE_REPLICA_ABORTED_RETRY_REQUEST,
        "no_available_replica_aborted_retry_request");
    return map;
  }
}
