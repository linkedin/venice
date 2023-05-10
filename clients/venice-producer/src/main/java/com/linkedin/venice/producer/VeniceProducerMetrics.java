package com.linkedin.venice.producer;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Total;


public class VeniceProducerMetrics extends AbstractVeniceStats {
  private final boolean enableMetrics;
  private Sensor requestSensor = null;
  private Sensor putRequestSensor = null;
  private Sensor deleteRequestSensor = null;
  private Sensor updateRequestSensor = null;
  private Sensor successRequestSensor = null;
  private Sensor failedRequestSensor = null;
  private Sensor produceLatencySensor = null;
  private Sensor pendingRequestSensor = null;

  public VeniceProducerMetrics(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);

    if (metricsRepository != null) {
      enableMetrics = true;
      requestSensor = registerSensor("write_request", new OccurrenceRate());
      putRequestSensor = registerSensor("put_request", new OccurrenceRate());
      deleteRequestSensor = registerSensor("delete_request", new OccurrenceRate());
      updateRequestSensor = registerSensor("update_request", new OccurrenceRate());

      successRequestSensor = registerSensor("success_write_request", new OccurrenceRate());
      failedRequestSensor = registerSensor("failed_write_request", new OccurrenceRate());
      produceLatencySensor = registerSensor("produce_latency", new Avg(), new Max());

      pendingRequestSensor = registerSensor("pending_write_request", new Total());
    } else {
      enableMetrics = false;
    }
  }

  private void recordRequest() {
    if (enableMetrics) {
      requestSensor.record();
      pendingRequestSensor.record(1);
    }
  }

  public void recordPutRequest() {
    if (enableMetrics) {
      recordRequest();
      putRequestSensor.record();
    }
  }

  public void recordDeleteRequest() {
    if (enableMetrics) {
      recordRequest();
      deleteRequestSensor.record();
    }
  }

  public void recordUpdateRequest() {
    if (enableMetrics) {
      recordRequest();
      updateRequestSensor.record();
    }
  }

  public void recordSuccessfulRequestWithLatency(long latencyMs) {
    if (enableMetrics) {
      successRequestSensor.record();
      produceLatencySensor.record(latencyMs);
      pendingRequestSensor.record(-1);
    }
  }

  public void recordFailedRequest() {
    if (enableMetrics) {
      failedRequestSensor.record();
      pendingRequestSensor.record(-1);
    }
  }
}
