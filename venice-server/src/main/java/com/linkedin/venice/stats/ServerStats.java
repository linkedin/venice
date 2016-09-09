package com.linkedin.venice.stats;

import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.SampledCount;

import javax.validation.constraints.NotNull;


/**
 * Created by athirupa on 8/16/16.
 */
public class ServerStats extends AbstractVeniceStats {
  //Sensors for measuring BnP job
  private final Sensor bytesConsumedSensor;
  private final Sensor recordsConsumedSensor;

  //Sensors for measuring requests from Router
  //request for data
  private final Sensor successRequestSensor;
  private final Sensor errorRequestSensor;
  private final Sensor successRequestLatencySensor;
  private final Sensor errorRequestLatencySensor;
  private final Sensor successRequestRatioSensor;

  private final Sensor bdbQueryLatencySensor;

  public ServerStats(@NotNull MetricsRepository metricsRepository, @NotNull String name) {
    super(metricsRepository, name);

    bytesConsumedSensor = registerSensor("bytes_consumed", new Rate());
    recordsConsumedSensor = registerSensor("records_consumed", new Rate());

    MeasurableStat successRequest = new SampledCount();
    MeasurableStat errorRequest = new SampledCount();
    successRequestSensor = registerSensor("success_request", successRequest, new OccurrenceRate());
    errorRequestSensor = registerSensor("error_request", errorRequest, new OccurrenceRate());
    successRequestLatencySensor = registerSensor("success_request_latency",
      TehutiUtils.getPercentileStat(getName() + "_" + "success_request_latency"));
    errorRequestLatencySensor = registerSensor("error_request_latency",
      TehutiUtils.getPercentileStat(getName() + "_" + "error_request_latency"));
    successRequestRatioSensor = registerSensor("success_request_ratio",
      TehutiUtils.getRatioStat(successRequest, errorRequest));

    //bdbQueryLatency is normally less than 1 ms. Record ns instead of ms for better readability.
    bdbQueryLatencySensor = registerSensor("bdb_query_latency_ns",
      TehutiUtils.getPercentileStat(getName() + "_" + "bdb_query_latency"));
  }

  public void recordBytesConsumed(long bytes) {
    bytesConsumedSensor.record(bytes);
  }

  public void recordRecordsConsumed(int count) {
    recordsConsumedSensor.record(count);
  }

  public void recordSuccessRequest() {
    record(successRequestSensor);
  }

  public void recordErrorRequest() {
    record(errorRequestSensor);
  }

  public void recordSuccessRequestLatency(double latency) {
    record(successRequestLatencySensor, latency);
  }

  public void recordErrorRequestLatency(double latency) {
    record(errorRequestLatencySensor, latency);
  }

  public void recordBdbQueryLatency(double latency) {
    record(bdbQueryLatencySensor, latency);
  }
}
