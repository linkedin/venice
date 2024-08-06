package com.linkedin.davinci.consumer.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;


public class BasicConsumerStats extends AbstractVeniceStats {
  private final Sensor maxLagSensor;
  private final Sensor recordsConsumed;
  private final Sensor maximumConsumingVersion;

  private final Sensor minimumConsumingVersion;

  public BasicConsumerStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    maxLagSensor = registerSensor("max_partition_lag", new Max());
    recordsConsumed = registerSensor("records_consumed", new Avg(), new Max());
    maximumConsumingVersion = registerSensor("maximum_consuming_version", new Gauge());
    minimumConsumingVersion = registerSensor("minimum_consuming_version", new Gauge());
  }

  public void recordMaximumConsumingVersion(int version) {
    maximumConsumingVersion.record(version);
  }

  public void recordMinimumConsumingVersion(int version) {
    minimumConsumingVersion.record(version);
  }

  public void recordLag(Long lag) {
    maxLagSensor.record(lag);
  }

  public void recordRecordsConsumed(int count) {
    recordsConsumed.record(count);
  }
}
