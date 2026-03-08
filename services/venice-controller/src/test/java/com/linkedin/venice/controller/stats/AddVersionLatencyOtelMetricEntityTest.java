package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_PROCESSING_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.AddVersionLatencyStats.AddVersionLatencyOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class AddVersionLatencyOtelMetricEntityTest
    extends AbstractModuleMetricEntityTest<AddVersionLatencyOtelMetricEntity> {
  public AddVersionLatencyOtelMetricEntityTest() {
    super(AddVersionLatencyOtelMetricEntity.class);
  }

  @Override
  protected Map<AddVersionLatencyOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<AddVersionLatencyOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        AddVersionLatencyOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME_PER_COMPONENT,
        new MetricEntityExpectation(
            "admin_consumption.message.phase.start_to_end_processing.time_per_component",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Per-component breakdown of start-to-end processing time",
            setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE, VENICE_ADMIN_MESSAGE_PROCESSING_COMPONENT)));
    return map;
  }
}
