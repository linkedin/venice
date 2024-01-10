package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;


public class PartitionOffsetFetcherStats extends AbstractVeniceStats {
  public enum OCCURRENCE_LATENCY_SENSOR_TYPE {
    GET_TOPIC_LATEST_OFFSETS, GET_PARTITION_LATEST_OFFSET_WITH_RETRY, GET_PARTITIONS_OFFSETS_BY_TIME,
    GET_PARTITION_OFFSET_BY_TIME, GET_LATEST_PRODUCER_TIMESTAMP_ON_DATA_RECORD_WITH_RETRY, PARTITIONS_FOR,
    GET_PARTITION_OFFSET_BY_TIME_IF_OUT_OF_RANGE
  }

  private final Map<OCCURRENCE_LATENCY_SENSOR_TYPE, Sensor> sensorsByTypes;
  private Sensor getPartitionLatestOffsetError;

  public PartitionOffsetFetcherStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    Map<OCCURRENCE_LATENCY_SENSOR_TYPE, Sensor> tmpRateSensorsByTypes =
        new EnumMap<>(OCCURRENCE_LATENCY_SENSOR_TYPE.class);
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
    this.getPartitionLatestOffsetError =
        registerSensorIfAbsent("get_partition_latest_offset_with_retry_error", new OccurrenceRate());
  }

  public void recordLatency(OCCURRENCE_LATENCY_SENSOR_TYPE sensor_type, long requestLatencyMs) {
    sensorsByTypes.get(sensor_type).record(requestLatencyMs);
  }

  public void recordGetLatestOffsetError() {
    this.getPartitionLatestOffsetError.record();
  }
}
