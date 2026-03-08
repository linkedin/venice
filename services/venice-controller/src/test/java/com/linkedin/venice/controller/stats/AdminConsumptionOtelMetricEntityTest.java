package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controller.stats.AdminConsumptionStats.AdminConsumptionOtelMetricEntity;
import com.linkedin.venice.stats.metrics.AbstractModuleMetricEntityTest;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.HashMap;
import java.util.Map;


public class AdminConsumptionOtelMetricEntityTest
    extends AbstractModuleMetricEntityTest<AdminConsumptionOtelMetricEntity> {
  public AdminConsumptionOtelMetricEntityTest() {
    super(AdminConsumptionOtelMetricEntity.class);
  }

  @Override
  protected Map<AdminConsumptionOtelMetricEntity, MetricEntityExpectation> expectedDefinitions() {
    Map<AdminConsumptionOtelMetricEntity, MetricEntityExpectation> map = new HashMap<>();
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_FAILURE_COUNT,
        new MetricEntityExpectation(
            "admin_consumption.message.failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of failed admin messages",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_RETRIABLE_FAILURE_COUNT,
        new MetricEntityExpectation(
            "admin_consumption.message.retriable_failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of retriable failed admin messages",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_DIV_FAILURE_COUNT,
        new MetricEntityExpectation(
            "admin_consumption.message.div_failure_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of admin message DIV error reports",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_FUTURE_SCHEMA_COUNT,
        new MetricEntityExpectation(
            "admin_consumption.message.future_schema_count",
            MetricType.COUNTER,
            MetricUnit.NUMBER,
            "Count of admin messages with future protocol versions requiring schema fetch",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_REPLICATION_TO_LOCAL_BROKER_TIME,
        new MetricEntityExpectation(
            "admin_consumption.message.phase.replication_to_local_broker.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time taken for the message from parent to be replicated to the child controller's admin topic",
            setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_BROKER_TO_PROCESSING_QUEUE_TIME,
        new MetricEntityExpectation(
            "admin_consumption.message.phase.broker_to_processing_queue.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time from local broker timestamp to delegation to processing queue",
            setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_QUEUE_TO_START_PROCESSING_TIME,
        new MetricEntityExpectation(
            "admin_consumption.message.phase.queue_to_start_processing.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time from delegation to first processing attempt",
            setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PHASE_START_TO_END_PROCESSING_TIME,
        new MetricEntityExpectation(
            "admin_consumption.message.phase.start_to_end_processing.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Time from start of processing to completion",
            setOf(VENICE_CLUSTER_NAME, VENICE_ADMIN_MESSAGE_TYPE)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_BATCH_PROCESSING_CYCLE_TIME,
        new MetricEntityExpectation(
            "admin_consumption.message.batch_processing_cycle.time",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.MILLISECOND,
            "Duration of batch processing cycle",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_MESSAGE_PENDING_COUNT,
        new MetricEntityExpectation(
            "admin_consumption.message.pending_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Pending admin messages remaining in the internal queue",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_STORE_PENDING_COUNT,
        new MetricEntityExpectation(
            "admin_consumption.store.pending_count",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Number of stores with pending admin messages",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_CONSUMER_OFFSET_LAG,
        new MetricEntityExpectation(
            "admin_consumption.consumer.offset_lag",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Difference between end offset and latest consumed offset",
            setOf(VENICE_CLUSTER_NAME)));
    map.put(
        AdminConsumptionOtelMetricEntity.ADMIN_CONSUMPTION_CONSUMER_CHECKPOINT_OFFSET_LAG,
        new MetricEntityExpectation(
            "admin_consumption.consumer.checkpoint_offset_lag",
            MetricType.ASYNC_GAUGE,
            MetricUnit.NUMBER,
            "Difference between end offset and latest persisted offset",
            setOf(VENICE_CLUSTER_NAME)));
    return map;
  }
}
