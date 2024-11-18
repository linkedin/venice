package com.linkedin.venice.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.VeniceMetricsConfig.Builder;
import io.opentelemetry.exporter.otlp.internal.OtlpConfigUtil;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.tehuti.metrics.MetricConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.MissingArgumentException;
import org.testng.annotations.Test;


public class VeniceMetricsConfigTest {
  @Test(expectedExceptions = MissingArgumentException.class)
  public void testDefaultValuesThrowsException() throws MissingArgumentException {
    new Builder().build();
  }

  @Test
  public void testDefaultValuesWithBasicConfig() throws MissingArgumentException {
    VeniceMetricsConfig config = new Builder().setServiceName("noop_service").setMetricPrefix("service").build();
    assertEquals(config.getServiceName(), "noop_service");
    assertEquals(config.getMetricPrefix(), "service");
    assertFalse(config.emitOtelMetrics());
    assertFalse(config.exportOtelMetricsToEndpoint());
    assertEquals(config.getOtelExportProtocol(), OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF);
    assertEquals(config.getOtelEndpoint(), null);
    assertTrue(config.getOtelHeaders().isEmpty());
    assertFalse(config.exportOtelMetricsToLog());
    assertEquals(config.getMetricNamingFormat(), VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE);
    assertEquals(config.getOtelAggregationTemporalitySelector(), AggregationTemporalitySelector.deltaPreferred());
    assertEquals(config.getOtelHistogramAggregationSelector(), null);
    assertNotNull(config.getTehutiMetricConfig());
  }

  @Test
  public void testCustomValues() throws MissingArgumentException {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.export.to.log", "true");

    MetricConfig metricConfig = new MetricConfig();

    VeniceMetricsConfig config = new Builder().setServiceName("TestService")
        .setMetricPrefix("TestPrefix")
        .setTehutiMetricConfig(metricConfig)
        .extractAndSetOtelConfigs(otelConfigs)
        .build();

    assertEquals(config.getServiceName(), "TestService");
    assertEquals(config.getMetricPrefix(), "TestPrefix");
    assertTrue(config.emitOtelMetrics());
    assertTrue(config.exportOtelMetricsToLog());
    assertEquals(config.getTehutiMetricConfig(), metricConfig);
  }

  @Test(expectedExceptions = MissingArgumentException.class)
  public void testOtelMissingConfigs() throws MissingArgumentException {
    Map<String, String> invalidOtelConfigs = new HashMap<>();
    invalidOtelConfigs.put("otel.venice.enabled", "true");
    invalidOtelConfigs.put("otel.venice.export.to.endpoint", "true");

    new Builder().setServiceName("TestService")
        .setMetricPrefix("TestPrefix")
        .extractAndSetOtelConfigs(invalidOtelConfigs)
        .build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOtelConfigWithInvalidMetricFormat() throws MissingArgumentException {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.metrics.format", "INVALID_FORMAT");

    new Builder().extractAndSetOtelConfigs(otelConfigs).build();
  }

  @Test
  public void testOtelConfigWithValidMetricFormat() throws MissingArgumentException {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.metrics.format", "CAMEL_CASE");

    VeniceMetricsConfig config = new Builder().setServiceName("TestService")
        .setMetricPrefix("TestPrefix")
        .extractAndSetOtelConfigs(otelConfigs)
        .build();

    assertEquals(config.getMetricNamingFormat(), VeniceOpenTelemetryMetricNamingFormat.CAMEL_CASE);
  }

  @Test
  public void testEnableHttpGrpcEndpointConfigWithRequiredFields() throws MissingArgumentException {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.export.to.endpoint", "true");
    otelConfigs.put("otel.exporter.otlp.metrics.protocol", OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF);
    otelConfigs.put("otel.exporter.otlp.metrics.endpoint", "http://localhost");

    VeniceMetricsConfig config = new Builder().setServiceName("TestService")
        .setMetricPrefix("TestPrefix")
        .extractAndSetOtelConfigs(otelConfigs)
        .build();

    assertTrue(config.exportOtelMetricsToEndpoint());
    assertEquals(config.getOtelExportProtocol(), OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF);
    assertEquals(config.getOtelEndpoint(), "http://localhost");
  }

  @Test
  public void testSetAggregationTemporalitySelector() throws MissingArgumentException {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.export.to.endpoint", "true");
    otelConfigs.put("otel.exporter.otlp.metrics.protocol", OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF);
    otelConfigs.put("otel.exporter.otlp.metrics.endpoint", "http://localhost");
    otelConfigs.put("otel.exporter.otlp.metrics.temporality.preference", "delta");

    VeniceMetricsConfig config = new Builder().setServiceName("TestService")
        .setMetricPrefix("TestPrefix")
        .extractAndSetOtelConfigs(otelConfigs)
        .build();
    assertEquals(config.getOtelAggregationTemporalitySelector(), AggregationTemporalitySelector.deltaPreferred());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetAggregationTemporalitySelectorInvalidConfig() throws MissingArgumentException {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.export.to.endpoint", "true");
    otelConfigs.put("otel.exporter.otlp.metrics.protocol", OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF);
    otelConfigs.put("otel.exporter.otlp.metrics.endpoint", "http://localhost");
    otelConfigs.put("otel.exporter.otlp.metrics.temporality.preference", "invalid");

    VeniceMetricsConfig config = new Builder().setServiceName("TestService")
        .setMetricPrefix("TestPrefix")
        .extractAndSetOtelConfigs(otelConfigs)
        .build();
    assertEquals(config.getOtelAggregationTemporalitySelector(), AggregationTemporalitySelector.deltaPreferred());
  }

  @Test
  public void testSetHistogramAggregationSelector() throws MissingArgumentException {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.export.to.endpoint", "true");
    otelConfigs.put("otel.exporter.otlp.metrics.protocol", OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF);
    otelConfigs.put("otel.exporter.otlp.metrics.endpoint", "http://localhost");
    otelConfigs.put("otel.exporter.otlp.metrics.default.histogram.aggregation", "base2_exponential_bucket_histogram");
    otelConfigs.put("otel.exporter.otlp.metrics.default.histogram.aggregation.max.scale", "10");
    otelConfigs.put("otel.exporter.otlp.metrics.default.histogram.aggregation.max.buckets", "50");

    VeniceMetricsConfig config = new Builder().setServiceName("TestService")
        .setMetricPrefix("TestPrefix")
        .extractAndSetOtelConfigs(otelConfigs)
        .build();
    assertEquals(
        config.getOtelHistogramAggregationSelector().getDefaultAggregation(InstrumentType.HISTOGRAM).toString(),
        "Base2ExponentialHistogramAggregation{maxBuckets=50,maxScale=10}");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetHistogramAggregationSelectorInvalidConfig() throws MissingArgumentException {
    Map<String, String> otelConfigs = new HashMap<>();
    otelConfigs.put("otel.venice.enabled", "true");
    otelConfigs.put("otel.venice.export.to.endpoint", "true");
    otelConfigs.put("otel.exporter.otlp.metrics.protocol", OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF);
    otelConfigs.put("otel.exporter.otlp.metrics.endpoint", "http://localhost");
    otelConfigs.put("otel.exporter.otlp.metrics.default.histogram.aggregation", "invalid");
    otelConfigs.put("otel.exporter.otlp.metrics.default.histogram.aggregation.max.scale", "10");
    otelConfigs.put("otel.exporter.otlp.metrics.default.histogram.aggregation.max.buckets", "50");

    new Builder().setServiceName("TestService")
        .setMetricPrefix("TestPrefix")
        .extractAndSetOtelConfigs(otelConfigs)
        .build();
  }
}
