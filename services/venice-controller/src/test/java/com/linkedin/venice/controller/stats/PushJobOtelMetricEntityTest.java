package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_DATA_WRITER_SINK;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.PushJobStatusStats.PushJobOtelMetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityTestFixture.MetricEntityExpectation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class PushJobOtelMetricEntityTest {
  private static Map<PushJobOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<PushJobOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT,
        new MetricEntityExpectation(
            "push_job.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Push job completions, differentiated by push type and status",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_PUSH_JOB_TYPE, VENICE_PUSH_JOB_STATUS)));
    map.put(
        PushJobOtelMetricEntity.PUSH_JOB_DATA_WRITER_SINK_WRITE_TIME,
        new MetricEntityExpectation(
            "push_job.data_writer.sink_write_time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Summed data writer task duration spent writing to a push job sink, differentiated by push type and sink",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_PUSH_JOB_TYPE, VENICE_PUSH_JOB_DATA_WRITER_SINK)));
    map.put(
        PushJobOtelMetricEntity.PUSH_JOB_EXTERNAL_STORAGE_WRITE_FAILURE_COUNT,
        new MetricEntityExpectation(
            "push_job.external_storage_write_failure.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Pushes that exhausted external storage write retries and failed the region's version storage mode open to internal, differentiated by region",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_REGION_NAME)));
    return map;
  }

  @Test
  public void testMetricEntities() {
    new ModuleMetricEntityTestFixture<>(PushJobOtelMetricEntity.class, expectedDefinitions()).assertAll();
  }
}
