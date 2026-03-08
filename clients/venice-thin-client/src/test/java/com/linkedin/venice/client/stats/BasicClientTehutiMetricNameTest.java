package com.linkedin.venice.client.stats;

import com.linkedin.venice.client.stats.BasicClientStats.BasicClientTehutiMetricName;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class BasicClientTehutiMetricNameTest extends AbstractTehutiMetricNameEnumTest<BasicClientTehutiMetricName> {
  public BasicClientTehutiMetricNameTest() {
    super(BasicClientTehutiMetricName.class);
  }

  @Override
  protected Map<BasicClientTehutiMetricName, String> expectedMetricNames() {
    Map<BasicClientTehutiMetricName, String> map = new HashMap<>();
    map.put(BasicClientTehutiMetricName.HEALTHY_REQUEST, "healthy_request");
    map.put(BasicClientTehutiMetricName.UNHEALTHY_REQUEST, "unhealthy_request");
    map.put(BasicClientTehutiMetricName.HEALTHY_REQUEST_LATENCY, "healthy_request_latency");
    map.put(BasicClientTehutiMetricName.UNHEALTHY_REQUEST_LATENCY, "unhealthy_request_latency");
    map.put(BasicClientTehutiMetricName.REQUEST_KEY_COUNT, "request_key_count");
    map.put(BasicClientTehutiMetricName.SUCCESS_REQUEST_KEY_COUNT, "success_request_key_count");
    return map;
  }
}
