package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class ParticipantStoreConsumptionOtelMetricEntityTest
    extends AbstractModuleMetricEntityTest<ParticipantStoreConsumptionOtelMetricEntity> {
  public ParticipantStoreConsumptionOtelMetricEntityTest() {
    super(ParticipantStoreConsumptionOtelMetricEntity.class);
  }

  @Override
  protected Map<ParticipantStoreConsumptionOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<ParticipantStoreConsumptionOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_LATENCY,
        new MetricEntityExpectation(
            "participant_store.consumption_task.push_job.kill.time",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Latency from kill signal generation in the child controller to kill execution on the storage node",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    map.put(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT,
        new MetricEntityExpectation(
            "participant_store.consumption_task.push_job.kill.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of push job kill attempts on the storage node",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)));
    map.put(
        ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_FAILED_CONSUMPTION_COUNT,
        new MetricEntityExpectation(
            "participant_store.consumption_task.consumption.failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of exceptions thrown during participant store consumption for kill push job records",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)));
    map.put(
        ParticipantStoreConsumptionOtelMetricEntity.FAILED_INITIALIZATION_COUNT,
        new MetricEntityExpectation(
            "participant_store.consumption_task.initialization.failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of participant store consumption task initialization failures",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        ParticipantStoreConsumptionOtelMetricEntity.HEARTBEAT_COUNT,
        new MetricEntityExpectation(
            "participant_store.consumption_task.heartbeat.count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Heartbeat count for the participant store consumption task",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }
}
