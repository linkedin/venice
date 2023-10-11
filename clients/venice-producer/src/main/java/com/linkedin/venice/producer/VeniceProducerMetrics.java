package com.linkedin.venice.producer;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Min;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.concurrent.atomic.AtomicInteger;


public class VeniceProducerMetrics extends AbstractVeniceStats {
  private final boolean enableMetrics;
  private Sensor operationSensor = null;
  private Sensor putOperationSensor = null;
  private Sensor deleteOperationSensor = null;
  private Sensor updateOperationSensor = null;
  private Sensor successOperationSensor = null;
  private Sensor failedOperationSensor = null;
  private Sensor produceLatencySensor = null;
  private Sensor pendingOperationSensor = null;

  private final AtomicInteger pendingOperationCounter = new AtomicInteger(0);

  public VeniceProducerMetrics(MetricsRepository metricsRepository, String storeName) {
    super(metricsRepository, storeName);

    if (metricsRepository != null) {
      enableMetrics = true;
      operationSensor = registerSensor("write_operation", new OccurrenceRate());
      putOperationSensor = registerSensor("put_operation", new OccurrenceRate());
      deleteOperationSensor = registerSensor("delete_operation", new OccurrenceRate());
      updateOperationSensor = registerSensor("update_operation", new OccurrenceRate());

      successOperationSensor = registerSensor("success_write_operation", new OccurrenceRate());
      failedOperationSensor = registerSensor("failed_write_operation", new OccurrenceRate());
      String produceLatencySensorName = "produce_to_durable_buffer_latency";
      produceLatencySensor = registerSensor(
          produceLatencySensorName,
          TehutiUtils.getPercentileStat(getName() + AbstractVeniceStats.DELIMITER + produceLatencySensorName));

      pendingOperationSensor = registerSensor("pending_write_operation", new Min(), new Max());
    } else {
      enableMetrics = false;
    }
  }

  private void recordRequest() {
    if (enableMetrics) {
      operationSensor.record();
      pendingOperationSensor.record(pendingOperationCounter.incrementAndGet());
    }
  }

  public void recordPutRequest() {
    if (enableMetrics) {
      recordRequest();
      putOperationSensor.record();
    }
  }

  public void recordDeleteRequest() {
    if (enableMetrics) {
      recordRequest();
      deleteOperationSensor.record();
    }
  }

  public void recordUpdateRequest() {
    if (enableMetrics) {
      recordRequest();
      updateOperationSensor.record();
    }
  }

  public void recordSuccessfulRequestWithLatency(long latencyMs) {
    if (enableMetrics) {
      successOperationSensor.record();
      produceLatencySensor.record(latencyMs);
      pendingOperationSensor.record(pendingOperationCounter.decrementAndGet());
    }
  }

  public void recordFailedRequest() {
    if (enableMetrics) {
      failedOperationSensor.record();
      pendingOperationSensor.record(pendingOperationCounter.decrementAndGet());
    }
  }
}
