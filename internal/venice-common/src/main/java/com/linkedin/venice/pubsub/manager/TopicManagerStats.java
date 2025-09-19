package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.LatencyUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Stats for topic manager operations
 */
class TopicManagerStats extends AbstractVeniceStats {
  private static final String TOPIC_MANAGER_STATS_PREFIX = "TopicManagerStats_";
  private final MetricsRepository metricsRepository;
  private final AtomicInteger pubSubAdminOpFailureCount = new AtomicInteger(0);
  private EnumMap<SENSOR_TYPE, Sensor> sensorsByTypes;

  enum SENSOR_TYPE {
    CREATE_TOPIC, DELETE_TOPIC, LIST_ALL_TOPICS, SET_TOPIC_CONFIG, GET_ALL_TOPIC_RETENTIONS, GET_TOPIC_CONFIG,
    GET_TOPIC_CONFIG_WITH_RETRY, CONTAINS_TOPIC, GET_SOME_TOPIC_CONFIGS, CONTAINS_TOPIC_WITH_RETRY,
    GET_TOPIC_START_POSITIONS, GET_TOPIC_END_POSITIONS, GET_PARTITION_START_POSITION, GET_PARTITION_END_POSITION,
    PARTITIONS_FOR, GET_OFFSET_FOR_TIME, CONSUMER_ACQUISITION_WAIT_TIME
  }

  // Subset of sensors that are applicable outside the controller (e.g., server).
  static final EnumSet<SENSOR_TYPE> SHARED_SENSORS = EnumSet.of(
      SENSOR_TYPE.CONTAINS_TOPIC,
      SENSOR_TYPE.CONTAINS_TOPIC_WITH_RETRY,
      SENSOR_TYPE.GET_TOPIC_END_POSITIONS,
      SENSOR_TYPE.GET_PARTITION_END_POSITION,
      SENSOR_TYPE.CONSUMER_ACQUISITION_WAIT_TIME);

  TopicManagerStats(MetricsRepository metricsRepository, String pubSubAddress, VeniceComponent component) {
    super(metricsRepository, TOPIC_MANAGER_STATS_PREFIX + TehutiUtils.fixMalformedMetricName(pubSubAddress));
    this.metricsRepository = metricsRepository;
    if (metricsRepository == null) {
      return;
    }
    sensorsByTypes = new EnumMap<>(SENSOR_TYPE.class);
    boolean isController = component == VeniceComponent.CONTROLLER;
    for (SENSOR_TYPE sensorType: SENSOR_TYPE.values()) {
      if (isController || SHARED_SENSORS.contains(sensorType)) {
        // Register sensors that are either controller-specific or shared.
        sensorsByTypes.put(
            sensorType,
            registerSensorIfAbsent(sensorType.name().toLowerCase(), new OccurrenceRate(), new Max(), new Avg()));
      }
    }
    registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> pubSubAdminOpFailureCount.getAndSet(0),
            "pub_sub_admin_op_failure_count"));
  }

  EnumMap<SENSOR_TYPE, Sensor> getSensorsByTypes() {
    return sensorsByTypes;
  }

  void recordLatency(SENSOR_TYPE sensorType, long startTimeInNs) {
    if (sensorsByTypes == null || sensorType == null) {
      return;
    }
    Sensor sensor = sensorsByTypes.get(sensorType);
    if (sensor == null) {
      return;
    }
    // convert ns to us and record
    sensor.record(LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNs));
  }

  void recordPubSubAdminOpFailure() {
    pubSubAdminOpFailureCount.incrementAndGet();
  }

  // visible for testing
  int getPubSubAdminOpFailureCount() {
    return pubSubAdminOpFailureCount.get();
  }

  final void registerTopicMetadataFetcherSensors(TopicMetadataFetcher topicMetadataFetcher) {
    if (metricsRepository == null) {
      return;
    }
    registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> topicMetadataFetcher.getCurrentConsumerPoolSize(),
            "available_consumer_count"));
    registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> topicMetadataFetcher.getAsyncTaskActiveThreadCount(),
            "async_task_active_thread_count"));
    registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> topicMetadataFetcher.getAsyncTaskQueueLength(),
            "async_task_queue_length"));
    registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> topicMetadataFetcher.getConsumerWaitListSize(),
            "consumer_wait_list_size"));
  }
}
