package com.linkedin.venice.stats.routing;

import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class HelixGroupTehutiMetricNameTest
    extends AbstractTehutiMetricNameEnumTest<HelixGroupStats.HelixGroupTehutiMetricName> {
  public HelixGroupTehutiMetricNameTest() {
    super(HelixGroupStats.HelixGroupTehutiMetricName.class);
  }

  @Override
  protected Map<HelixGroupStats.HelixGroupTehutiMetricName, String> expectedMetricNames() {
    Map<HelixGroupStats.HelixGroupTehutiMetricName, String> map = new HashMap<>();
    map.put(HelixGroupStats.HelixGroupTehutiMetricName.GROUP_COUNT, "group_count");
    map.put(HelixGroupStats.HelixGroupTehutiMetricName.GROUP_REQUEST, "group_request");
    map.put(HelixGroupStats.HelixGroupTehutiMetricName.GROUP_PENDING_REQUEST, "group_pending_request");
    map.put(HelixGroupStats.HelixGroupTehutiMetricName.GROUP_RESPONSE_WAITING_TIME, "group_response_waiting_time");
    return map;
  }
}
