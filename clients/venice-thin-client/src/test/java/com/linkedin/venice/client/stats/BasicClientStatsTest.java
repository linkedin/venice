package com.linkedin.venice.client.stats;

import static org.testng.Assert.*;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.Test;


public class BasicClientStatsTest {
  @Test
  public void testMetricPrefix() {
    String storeName = "test_store";
    MetricsRepository metricsRepository1 = new MetricsRepository();
    // Without prefix
    ClientConfig config1 = new ClientConfig(storeName);
    BasicClientStats.getClientStats(metricsRepository1, storeName, RequestType.SINGLE_GET, config1);
    // Check metric name
    assertTrue(metricsRepository1.metrics().size() > 0);
    String metricPrefix1 = "." + storeName;
    metricsRepository1.metrics().forEach((k, v) -> {
      assertTrue(k.startsWith(metricPrefix1));
    });

    // With prefix
    String prefix = "test_prefix";
    MetricsRepository metricsRepository2 = new MetricsRepository();
    ClientConfig config2 = new ClientConfig(storeName).setStatsPrefix(prefix);
    BasicClientStats.getClientStats(metricsRepository2, storeName, RequestType.SINGLE_GET, config2);
    // Check metric name
    assertTrue(metricsRepository2.metrics().size() > 0);
    String metricPrefix2 = "." + prefix + "_" + storeName;
    String finalMetricPrefix = metricPrefix2;
    metricsRepository2.metrics().forEach((k, v) -> {
      assertTrue(k.startsWith(finalMetricPrefix));
    });

    // With prefix
    prefix = "venice_system_store_meta_store_abc";
    config2 = new ClientConfig(storeName).setStatsPrefix(prefix);
    ClientStats clientStats =
        ClientStats.getClientStats(metricsRepository2, storeName, RequestType.SINGLE_GET, config2);
    clientStats.recordStreamingResponseTimeToReceive99PctRecord(2);
    clientStats.recordRequestSerializationTime(2);
    clientStats.recordRequestSubmissionToResponseHandlingTime(1);
    clientStats.recordResponseDeserializationTime(2);
    clientStats.recordRequestRetryCount();
    clientStats.recordStreamingResponseTimeToReceiveFirstRecord(2);
    assertNull(metricsRepository2.getMetric("request_serialization_time"));
    assertNull(metricsRepository2.getMetric("response_tt99pr"));
    assertNull(metricsRepository2.getMetric("request_retry_count"));
    assertNull(metricsRepository2.getMetric("response_deserialization_time"));
  }
}
