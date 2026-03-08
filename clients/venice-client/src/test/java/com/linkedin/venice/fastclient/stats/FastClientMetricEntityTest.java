package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_FANOUT_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_REJECTION_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class FastClientMetricEntityTest extends AbstractModuleMetricEntityTest<FastClientMetricEntity> {
  public FastClientMetricEntityTest() {
    super(FastClientMetricEntity.class);
  }

  @Override
  protected Map<FastClientMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<FastClientMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        FastClientMetricEntity.RETRY_REQUEST_WIN_COUNT,
        new MetricEntityExpectation(
            "retry.request.win_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of retry requests which won",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)));
    map.put(
        FastClientMetricEntity.METADATA_STALENESS_DURATION,
        new MetricEntityExpectation(
            "metadata.staleness_duration",
            MetricType.ASYNC_GAUGE,
            MetricUnit.MILLISECOND,
            "High watermark of metadata staleness in ms",
            setOf(VENICE_STORE_NAME)));
    map.put(
        FastClientMetricEntity.REQUEST_FANOUT_COUNT,
        new MetricEntityExpectation(
            "request.fanout_count",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Fanout size for requests",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_FANOUT_TYPE)));
    map.put(
        FastClientMetricEntity.REQUEST_REJECTION_COUNT,
        new MetricEntityExpectation(
            "request.rejection_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of requests rejected by the client",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_REJECTION_REASON)));
    map.put(
        FastClientMetricEntity.REQUEST_REJECTION_RATIO,
        new MetricEntityExpectation(
            "request.rejection_ratio",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "Ratio of requests rejected by the client to total requests",
            setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_REQUEST_REJECTION_REASON)));
    return map;
  }
}
