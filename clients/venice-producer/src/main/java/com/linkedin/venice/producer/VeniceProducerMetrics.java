package com.linkedin.venice.producer;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Total;


public class VeniceProducerMetrics extends AbstractVeniceStats {
  private final Sensor requestSensor;
  private final Sensor putRequestSensor;
  private final Sensor deleteRequestSensor;
  private final Sensor updateRequestSensor;
  private final Sensor successRequestSensor;
  private final Sensor failedRequestSensor;
  private final Sensor produceLatencySensor;
  private final Sensor pendingRequestSensor;

  public VeniceProducerMetrics(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);

    requestSensor = registerSensor("write_request", new OccurrenceRate());
    putRequestSensor = registerSensor("put_request", new OccurrenceRate());
    deleteRequestSensor = registerSensor("delete_request", new OccurrenceRate());
    updateRequestSensor = registerSensor("update_request", new OccurrenceRate());

    successRequestSensor = registerSensor("success_write_request", new OccurrenceRate());
    failedRequestSensor = registerSensor("failed_write_request", new OccurrenceRate());
    produceLatencySensor = registerSensor("produce_latency", new Avg(), new Max());

    pendingRequestSensor = registerSensor("pending_write_request", new Total());
  }

  private void recordRequest() {
    requestSensor.record();
    pendingRequestSensor.record(1);
  }

  public void recordPutRequest() {
    recordRequest();
    putRequestSensor.record();
  }

  public void recordDeleteRequest() {
    recordRequest();
    deleteRequestSensor.record();
  }

  public void recordUpdateRequest() {
    recordRequest();
    updateRequestSensor.record();
  }

  public void recordSuccessfulRequestWithLatency(long latencyMs) {
    successRequestSensor.record();
    produceLatencySensor.record(latencyMs);
    pendingRequestSensor.record(-1);
  }

  public void recordFailedRequest() {
    failedRequestSensor.record();
    pendingRequestSensor.record(-1);
  }
}
