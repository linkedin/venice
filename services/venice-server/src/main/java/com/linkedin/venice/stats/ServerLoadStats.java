package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;


public class ServerLoadStats extends AbstractVeniceStats {
  private final Sensor totalRequestSensor;
  private final Sensor rejectedRequestSensor;
  private final Sensor acceptedRequestSensor;
  private final Sensor rejectionRatioSensor;

  public ServerLoadStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    totalRequestSensor = registerSensorIfAbsent("total_request", new OccurrenceRate());
    rejectedRequestSensor = registerSensorIfAbsent("rejected_request", new OccurrenceRate());
    acceptedRequestSensor = registerSensorIfAbsent("accepted_request", new OccurrenceRate());
    rejectionRatioSensor = registerSensorIfAbsent("rejection_ratio", new Avg(), new Max());
  }

  public void recordTotalRequest() {
    totalRequestSensor.record();
  }

  public void recordRejectedRequest() {
    rejectedRequestSensor.record();
  }

  public void recordAcceptedRequest() {
    acceptedRequestSensor.record();
  }

  public void recordRejectionRatio(double rejectionRatio) {
    rejectionRatioSensor.record(rejectionRatio);
  }
}
