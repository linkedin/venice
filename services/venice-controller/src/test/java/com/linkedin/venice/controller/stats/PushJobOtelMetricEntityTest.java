package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.PushJobStatusStats.PushJobOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class PushJobOtelMetricEntityTest extends AbstractModuleMetricEntityTest<PushJobOtelMetricEntity> {
  public PushJobOtelMetricEntityTest() {
    super(PushJobOtelMetricEntity.class);
  }

  @Override
  protected Map<PushJobOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<PushJobOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT,
        new MetricEntityExpectation(
            "push_job.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Push job completions, differentiated by push type and status",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_PUSH_JOB_TYPE, VENICE_PUSH_JOB_STATUS)));
    return map;
  }
}
