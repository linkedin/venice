package com.linkedin.venice.stats;

import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class KafkaAdminWrapperStats extends AbstractVeniceStats {
  private static final Map<Pair<MetricsRepository, String>, KafkaAdminWrapperStats> KAFKA_ADMIN_WRAPPER_STATS_SINGLETON_MAP =
      new VeniceConcurrentHashMap<>();

  public enum OCCURRENCE_LATENCY_SENSOR_TYPE {
    CREATE_TOPIC, DELETE_TOPIC, LIST_ALL_TOPICS, SET_TOPIC_CONFIG, GET_ALL_TOPIC_RETENTIONS, GET_TOPIC_CONFIG,
    GET_TOPIC_CONFIG_WITH_RETRY, CONTAINS_TOPIC, GET_SOME_TOPIC_CONFIGS, @Deprecated
    IS_TOPIC_DELETION_UNDER_WAY, DESCRIBE_TOPICS, CONTAINS_TOPIC_WITH_RETRY, CLOSE
  }

  private final Map<OCCURRENCE_LATENCY_SENSOR_TYPE, Sensor> sensorsByTypes;

  /**
   * This singleton function will guarantee for a unique pair of MetricsRepository and stat prefix,
   * there should be only one instance of {@link KafkaAdminWrapperStats} created.
   * This is trying to avoid the metric registration conflicts caused by multiple instances of this class.
   *
   * For other {@link AbstractVeniceStats} implementations, if it is not easy to pass around a singleton
   * among different classes, they could choose to adopt this singleton pattern.
   */
  public static KafkaAdminWrapperStats getInstance(MetricsRepository metricsRepository, String resourceName) {
    return KAFKA_ADMIN_WRAPPER_STATS_SINGLETON_MAP.computeIfAbsent(
        Pair.create(metricsRepository, TehutiUtils.fixMalformedMetricName(resourceName)),
        k -> new KafkaAdminWrapperStats(k.getFirst(), k.getSecond()));
  }

  private KafkaAdminWrapperStats(MetricsRepository metricsRepository, String resourceName) {
    super(metricsRepository, resourceName);
    Map<OCCURRENCE_LATENCY_SENSOR_TYPE, Sensor> tmpRateSensorsByTypes =
        new HashMap<>(OCCURRENCE_LATENCY_SENSOR_TYPE.values().length);
    for (OCCURRENCE_LATENCY_SENSOR_TYPE sensorType: OCCURRENCE_LATENCY_SENSOR_TYPE.values()) {
      final String sensorName = sensorType.name().toLowerCase();
      tmpRateSensorsByTypes.put(
          sensorType,
          registerSensorIfAbsent(
              sensorName,
              new OccurrenceRate(),
              new Max(),
              new Min(),
              new Avg(),
              TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + sensorName)));
    }

    this.sensorsByTypes = Collections.unmodifiableMap(tmpRateSensorsByTypes);
  }

  public void recordLatency(OCCURRENCE_LATENCY_SENSOR_TYPE sensor_type, long requestLatencyMs) {
    sensorsByTypes.get(sensor_type).record(requestLatencyMs);
  }
}
