package com.linkedin.venice.controller.stats;

import com.linkedin.venice.controller.stats.ProtocolVersionAutoDetectionStats.ProtocolVersionAutoDetectionTehutiMetricNameEnum;
import com.linkedin.venice.stats.metrics.AbstractTehutiMetricNameEnumTest;
import java.util.HashMap;
import java.util.Map;


public class ProtocolVersionAutoDetectionTehutiMetricNameEnumTest
    extends AbstractTehutiMetricNameEnumTest<ProtocolVersionAutoDetectionTehutiMetricNameEnum> {
  public ProtocolVersionAutoDetectionTehutiMetricNameEnumTest() {
    super(ProtocolVersionAutoDetectionTehutiMetricNameEnum.class);
  }

  @Override
  protected Map<ProtocolVersionAutoDetectionTehutiMetricNameEnum, String> expectedMetricNames() {
    Map<ProtocolVersionAutoDetectionTehutiMetricNameEnum, String> map = new HashMap<>();
    map.put(
        ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_ERROR,
        "protocol_version_auto_detection_error");
    map.put(
        ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_LATENCY,
        "protocol_version_auto_detection_latency");
    return map;
  }
}
