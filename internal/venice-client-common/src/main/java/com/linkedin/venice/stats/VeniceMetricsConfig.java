package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.VeniceException;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.tehuti.metrics.MetricConfig;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceMetricsConfig {
  private static final Logger LOGGER = LogManager.getLogger(VeniceMetricsConfig.class);
  private final String serviceName;
  private final String metricPrefix;
  /** config to control whether to emit OpenTelemetry or tehuti metrics or both
   * emitTehutiMetrics is not used for now */
  private final boolean emitOpenTelemetryMetrics;
  private final boolean emitTehutiMetrics;

  /** extra configs for OpenTelemetry. Supports 2 exporter currently
   * 1. {@link MetricExporter} for exporting to Http/Grpc endpoint. More details are supported via configs,
   *    check {@link VeniceMetricsConfigBuilder#extractAndSetOtelConfigs} and {@link VeniceOpenTelemetryMetricsRepository#getOtlpHttpMetricExporter}
   * 2. {@link VeniceOpenTelemetryMetricsRepository.LogBasedMetricExporter} for debug purposes
   */
  private final Map<String, String> otelConfigs;
  private final boolean emitToHttpGrpcEndpoint;
  private final boolean emitToLog; // for debug purposes
  private final VeniceOpenTelemetryMetricFormat metricFormat;
  private final boolean useExponentialHistogram;
  private final int exponentialHistogramMaxScale;
  private final int exponentialHistogramMaxBuckets;

  /** reusing tehuti's MetricConfig */
  private final MetricConfig tehutiMetricConfig;

  private VeniceMetricsConfig(VeniceMetricsConfigBuilder veniceMetricsConfigBuilder) {
    this.serviceName = veniceMetricsConfigBuilder.serviceName;
    this.metricPrefix = veniceMetricsConfigBuilder.metricPrefix;
    this.emitOpenTelemetryMetrics = veniceMetricsConfigBuilder.emitOpenTelemetryMetrics;
    this.emitTehutiMetrics = veniceMetricsConfigBuilder.emitTehutiMetrics;
    this.emitToHttpGrpcEndpoint = veniceMetricsConfigBuilder.emitToHttpGrpcEndpoint;
    this.emitToLog = veniceMetricsConfigBuilder.emitToLog;
    this.metricFormat = veniceMetricsConfigBuilder.metricFormat;
    this.useExponentialHistogram = veniceMetricsConfigBuilder.useExponentialHistogram;
    this.exponentialHistogramMaxScale = veniceMetricsConfigBuilder.exponentialHistogramMaxScale;
    this.exponentialHistogramMaxBuckets = veniceMetricsConfigBuilder.exponentialHistogramMaxBuckets;
    this.otelConfigs = veniceMetricsConfigBuilder.otelConfigs;
    this.tehutiMetricConfig = veniceMetricsConfigBuilder.tehutiMetricConfig;
  }

  public static class VeniceMetricsConfigBuilder {
    private String serviceName = "NOOP_SERVICE";
    private String metricPrefix = null;
    private boolean emitOpenTelemetryMetrics = false;
    private boolean emitTehutiMetrics = true;
    private boolean emitToHttpGrpcEndpoint = false;
    private boolean emitToLog = false;
    private VeniceOpenTelemetryMetricFormat metricFormat = VeniceOpenTelemetryMetricFormat.SNAKE_CASE;
    private boolean useExponentialHistogram = true;
    private int exponentialHistogramMaxScale = 3;
    private int exponentialHistogramMaxBuckets = 250;
    private Map<String, String> otelConfigs = new HashMap<>();
    private MetricConfig tehutiMetricConfig = null;

    public VeniceMetricsConfigBuilder setServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public VeniceMetricsConfigBuilder setMetricPrefix(String metricPrefix) {
      this.metricPrefix = metricPrefix;
      return this;
    }

    public VeniceMetricsConfigBuilder extractAndSetOtelConfigs(Map<String, String> configs) {
      // copy only OpenTelemetry related configs
      for (Map.Entry<String, String> entry: configs.entrySet()) {
        if (entry.getKey().startsWith("otel.")) {
          otelConfigs.put(entry.getKey(), entry.getValue());
        }
      }
      LOGGER.info("OpenTelemetry configs: {}", otelConfigs);
      return this;
    }

    public VeniceMetricsConfigBuilder setTehutiMetricConfig(MetricConfig tehutiMetricConfig) {
      this.tehutiMetricConfig = tehutiMetricConfig;
      return this;
    }

    /** get the last part of the service name
     * For instance: if service name is "venice-router", return "router"
     */
    public static String getMetricsPrefix(String input) {
      String[] parts = input.split("[\\-\\._]");
      String lastPart = parts[parts.length - 1];
      return lastPart;
    }

    // Validate required fields before building
    private void checkAndSetDefaults() {
      if (tehutiMetricConfig == null) {
        tehutiMetricConfig = new MetricConfig();
      }
      if (metricPrefix == null) {
        metricPrefix = getMetricsPrefix(serviceName);
      }
      if (otelConfigs.containsKey("otel.venice.enabled")) {
        String status = otelConfigs.get("otel.venice.enabled");
        if (status != null) {
          emitOpenTelemetryMetrics = status.toLowerCase(Locale.ROOT).equals("true");
        }
      }
      // check otelConfigs and set defaults
      if (emitOpenTelemetryMetrics) {
        if (otelConfigs.containsKey("otel.venice.metric.format")) {
          String format = otelConfigs.get("otel.venice.metric.format");
          if (format != null) {
            try {
              metricFormat = VeniceOpenTelemetryMetricFormat.valueOf(format.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
              LOGGER.warn("Invalid metric format: {}, setting to default: {}", format, metricFormat);
            }
          }
        }
        if (otelConfigs.containsKey("otel.venice.export.to.http.grpc.endpoint")) {
          String emitStatus = otelConfigs.get("otel.venice.export.to.http.grpc.endpoint");
          if (emitStatus != null) {
            emitToHttpGrpcEndpoint = emitStatus.toLowerCase(Locale.ROOT).equals("true");
          }
        }
        if (emitToHttpGrpcEndpoint) {
          if (!otelConfigs.containsKey("otel.exporter.otlp.metrics.protocol")
              || !otelConfigs.containsKey("otel.exporter.otlp.metrics.endpoint")) {
            throw new VeniceException(
                "otel settings missing for otel.exporter.otlp.metrics.protocol and otel.exporter.otlp.metrics.endpoint");
          }
        }
      }
    }

    public VeniceMetricsConfig build() {
      checkAndSetDefaults();
      return new VeniceMetricsConfig(this);
    }
  }

  // all getters
  public String getServiceName() {
    return this.serviceName;
  }

  public String getMetricPrefix() {
    return this.metricPrefix;
  }

  public boolean isEmitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  public boolean isEmitToHttpGrpcEndpoint() {
    return emitToHttpGrpcEndpoint;
  }

  public boolean isEmitToLog() {
    return emitToLog;
  }

  public VeniceOpenTelemetryMetricFormat getMetricFormat() {
    return metricFormat;
  }

  public boolean isUseExponentialHistogram() {
    return useExponentialHistogram;
  }

  public int getExponentialHistogramMaxScale() {
    return exponentialHistogramMaxScale;
  }

  public int getExponentialHistogramMaxBuckets() {
    return exponentialHistogramMaxBuckets;
  }

  public Map<String, String> getOtelConfigs() {
    return otelConfigs;
  }

  public MetricConfig getTehutiMetricConfig() {
    return tehutiMetricConfig;
  }

  @Override
  public String toString() {
    return "VeniceMetricsConfig{" + "serviceName='" + serviceName + '\'' + ", metricPrefix='" + metricPrefix + '\''
        + ", emitOpenTelemetryMetrics=" + emitOpenTelemetryMetrics + ", emitTehutiMetrics=" + emitTehutiMetrics
        + ", otelConfigs=" + otelConfigs + ", emitToHttpGrpcEndpoint=" + emitToHttpGrpcEndpoint + ", emitToLog="
        + emitToLog + ", metricFormat=" + metricFormat + ", useExponentialHistogram=" + useExponentialHistogram
        + ", exponentialHistogramMaxScale=" + exponentialHistogramMaxScale + ", exponentialHistogramMaxBuckets="
        + exponentialHistogramMaxBuckets + ", tehutiMetricConfig=" + tehutiMetricConfig + '}';
  }
}
