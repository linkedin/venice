package com.linkedin.venice.stats;

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

  public enum OCCURRENCE_LATENCY_SENSOR_TYPE {
    CREATE_TOPIC,
    DELETE_TOPIC,
    LIST_ALL_TOPICS,
    SET_TOPIC_CONFIG,
    GET_ALL_TOPIC_RETENTIONS,
    GET_TOPIC_CONFIG,
    GET_TOPIC_CONFIG_WITH_RETRY,
    CONTAINS_TOPIC,
    GET_ALL_TOPIC_CONFIG,
    IS_TOPIC_DELETION_UNDER_WAY,
    DESCRIBE_TOPICS,
    CONTAINS_TOPIC_WITH_RETRY
  }

  private final Map<OCCURRENCE_LATENCY_SENSOR_TYPE, Sensor> sensorsByTypes;

  public KafkaAdminWrapperStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    Map<OCCURRENCE_LATENCY_SENSOR_TYPE, Sensor> tmpRateSensorsByTypes = new HashMap<>(OCCURRENCE_LATENCY_SENSOR_TYPE.values().length);
    for (OCCURRENCE_LATENCY_SENSOR_TYPE sensorType : OCCURRENCE_LATENCY_SENSOR_TYPE.values()) {
      final String sensorName = sensorType.name().toLowerCase();
      tmpRateSensorsByTypes.put(
          sensorType,
          registerSensorIfAbsent(
              sensorName,
              new OccurrenceRate(),
              new Max(),
              new Min(),
              new Avg(),
              TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + sensorName)
          )
      );
    }

    this.sensorsByTypes = Collections.unmodifiableMap(tmpRateSensorsByTypes);
  }

  public void recordLatency(OCCURRENCE_LATENCY_SENSOR_TYPE sensor_type, long requestLatencyMs) {
    sensorsByTypes.get(sensor_type).record(requestLatencyMs);
  }
}
