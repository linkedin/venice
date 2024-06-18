package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.LatencyUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Stats for topic manager operations
 */
class TopicManagerStats extends AbstractVeniceStats {
  private static final String TOPIC_MANAGER_STATS_PREFIX = "TopicManagerStats_";
  private EnumMap<SENSOR_TYPE, Sensor> sensorsByTypes = null;
  private final MetricsRepository metricsRepository;
  private AtomicInteger pubSubAdminOpFailureCount = new AtomicInteger(0);

  enum SENSOR_TYPE {
    CREATE_TOPIC, DELETE_TOPIC, LIST_ALL_TOPICS, SET_TOPIC_CONFIG, GET_ALL_TOPIC_RETENTIONS, GET_TOPIC_CONFIG,
    GET_TOPIC_CONFIG_WITH_RETRY, CONTAINS_TOPIC, GET_SOME_TOPIC_CONFIGS, CONTAINS_TOPIC_WITH_RETRY,
    GET_TOPIC_LATEST_OFFSETS, GET_PARTITION_LATEST_OFFSETS, PARTITIONS_FOR, GET_OFFSET_FOR_TIME,
    GET_PRODUCER_TIMESTAMP_OF_LAST_DATA_MESSAGE, CONSUMER_ACQUISITION_WAIT_TIME
  }

  TopicManagerStats(MetricsRepository metricsRepository, String pubSubAddress) {
    super(metricsRepository, TOPIC_MANAGER_STATS_PREFIX + TehutiUtils.fixMalformedMetricName(pubSubAddress));
    this.metricsRepository = metricsRepository;
    if (metricsRepository == null) {
      return;
    }
    sensorsByTypes = new EnumMap<>(SENSOR_TYPE.class);
    for (SENSOR_TYPE sensorType: SENSOR_TYPE.values()) {
      final String sensorName = sensorType.name().toLowerCase();
      sensorsByTypes.put(
          sensorType,
          registerSensorIfAbsent(
              sensorName,
              new OccurrenceRate(),
              new Max(),
              new Min(),
              new Avg(),
              TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + sensorName)));
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
    // convert ns to us and record
    sensorsByTypes.get(sensorType).record(LatencyUtils.getElapsedTimeFromNSToMS(startTimeInNs));
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
