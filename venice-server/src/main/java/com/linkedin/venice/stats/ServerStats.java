package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import javax.validation.constraints.NotNull;


/**
 * Created by athirupa on 8/16/16.
 */
public class ServerStats extends AbstractVeniceStats {
  private final Sensor bytesConsumedSensor;

  private final Sensor recordsConsumedSensor;

  public ServerStats(@NotNull MetricsRepository metricsRepository, @NotNull String name) {
    super(metricsRepository, name);

    bytesConsumedSensor = registerSensor("bytes_consumed", new Rate());
    recordsConsumedSensor = registerSensor("records_consumed", new Rate());
  }

  public void addBytesConsumed(long bytes) {
    bytesConsumedSensor.record(bytes);
  }

  public void addRecordsConsumed(int count) {
    recordsConsumedSensor.record(count);
  }
}
