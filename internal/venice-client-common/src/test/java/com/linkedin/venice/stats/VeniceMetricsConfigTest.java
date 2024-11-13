package com.linkedin.venice.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.VeniceMetricsConfig.VeniceMetricsConfigBuilder;
import io.tehuti.metrics.MetricConfig;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class VeniceMetricsConfigTest {
  @Test
  public void testDefaultValues() {
    VeniceMetricsConfig config = new VeniceMetricsConfigBuilder().build();
    assertEquals(config.getServiceName(), "noop_service");
    assertEquals(config.getMetricPrefix(), "service");
    assertFalse(config.isEmitOpenTelemetryMetrics());
    assertFalse(config.isEmitToHttpGrpcEndpoint());
    assertFalse(config.isEmitToLog());
    assertTrue(config.isUseExponentialHistogram());
    assertEquals(config.getExponentialHistogramMaxScale(), 3);
    assertEquals(config.getExponentialHistogramMaxBuckets(), 250);
  }

  @Test
  public void testCustomValues() {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.export.to.log", "true");

    MetricConfig metricConfig = new MetricConfig();

    VeniceMetricsConfig config = new VeniceMetricsConfigBuilder().setServiceName("TestService")
        .setMetricPrefix("TestPrefix")
        .setTehutiMetricConfig(metricConfig)
        .extractAndSetOtelConfigs(otelConfigs)
        .build();

    assertEquals(config.getServiceName(), "TestService");
    assertEquals(config.getMetricPrefix(), "TestPrefix");
    assertTrue(config.isEmitOpenTelemetryMetrics());
    assertTrue(config.getOtelConfigs().containsKey("otel.venice.enabled"));
    assertTrue(config.isEmitToLog());
    assertEquals(config.getTehutiMetricConfig(), metricConfig);
  }

  @Test
  public void testOtelMissingConfigs() {
    Map<String, String> invalidOtelConfigs = new HashMap<>();
    invalidOtelConfigs.put("otel.venice.enabled", "true");
    invalidOtelConfigs.put("otel.venice.export.to.http.grpc.endpoint", "true");

    VeniceMetricsConfigBuilder builder = new VeniceMetricsConfigBuilder().extractAndSetOtelConfigs(invalidOtelConfigs);

    // should throw exception because required configs are missing
    assertThrows(VeniceException.class, builder::build);
  }

  @Test
  public void testGetMetricsPrefix() {
    assertEquals(VeniceMetricsConfigBuilder.getMetricsPrefix("venice-router"), "router");
    assertEquals(VeniceMetricsConfigBuilder.getMetricsPrefix("service_name"), "name");
    assertEquals(VeniceMetricsConfigBuilder.getMetricsPrefix("com.linkedin.service"), "service");
  }

  @Test
  public void testOtelConfigWithInvalidMetricFormat() {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.metrics.format", "INVALID_FORMAT");

    VeniceMetricsConfig config = new VeniceMetricsConfigBuilder().extractAndSetOtelConfigs(otelConfigs).build();

    assertEquals(
        config.getMetricFormat(),
        VeniceOpenTelemetryMetricFormat.SNAKE_CASE,
        "Invalid metric format should fall back to default.");
  }

  @Test
  public void testOtelConfigWithValidMetricFormat() {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.metrics.format", "CAMEL_CASE");

    VeniceMetricsConfig config = new VeniceMetricsConfigBuilder().extractAndSetOtelConfigs(otelConfigs).build();

    assertEquals(config.getMetricFormat(), VeniceOpenTelemetryMetricFormat.CAMEL_CASE);
  }

  @Test
  public void testEnableHttpGrpcEndpointConfigWithRequiredFields() {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.export.to.http.grpc.endpoint", "true");
    otelConfigs.put("otel.exporter.otlp.metrics.protocol", "http/protobuf");
    otelConfigs.put("otel.exporter.otlp.metrics.endpoint", "http://localhost");

    VeniceMetricsConfig config = new VeniceMetricsConfigBuilder().extractAndSetOtelConfigs(otelConfigs).build();

    assertTrue(config.isEmitToHttpGrpcEndpoint());
  }
}
