package com.linkedin.venice.stats;

import io.opentelemetry.exporter.otlp.internal.OtlpConfigUtil;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.DefaultAggregationSelector;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.tehuti.metrics.MetricConfig;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceMetricsConfig {
  private static final Logger LOGGER = LogManager.getLogger(VeniceMetricsConfig.class);

  /**
   * Config to enable OpenTelemetry metrics
   */
  public static final String OTEL_VENICE_ENABLED = "otel.venice.enabled";

  /**
   * Config to set the naming format for OpenTelemetry metrics
   * {@link VeniceOpenTelemetryMetricNamingFormat}
   */
  public static final String OTEL_VENICE_METRICS_NAMING_FORMAT = "otel.venice.metrics.naming.format";

  /**
   * Export opentelemetry metrics to a log exporter
   * {@link VeniceOpenTelemetryMetricsRepository.LogBasedMetricExporter}
   */
  public static final String OTEL_VENICE_EXPORT_TO_LOG = "otel.venice.export.to.log";

  /**
   * Export opentelemetry metrics to {@link #OTEL_EXPORTER_OTLP_METRICS_ENDPOINT}
   * over {@link #OTEL_EXPORTER_OTLP_METRICS_PROTOCOL}
   */
  public static final String OTEL_VENICE_EXPORT_TO_ENDPOINT = "otel.venice.export.to.endpoint";

  /**
   * Protocol over which the metrics are exported to {@link #OTEL_EXPORTER_OTLP_METRICS_ENDPOINT} <br>
   * 1. {@link OtlpConfigUtil#PROTOCOL_HTTP_PROTOBUF}  => "http/protobuf" <br>
   * 2. {@link OtlpConfigUtil#PROTOCOL_GRPC}  => "grpc"
   */
  public static final String OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = "otel.exporter.otlp.metrics.protocol";

  /**
   * The Endpoint to which the metrics are exported
   */
  public static final String OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = "otel.exporter.otlp.metrics.endpoint";

  /**
   * Additional headers to pass while creating OpenTelemetry exporter
   */
  public static final String OTEL_EXPORTER_OTLP_METRICS_HEADERS = "otel.exporter.otlp.metrics.headers";

  /**
   * Aggregation Temporality selector to export only the delta or cumulate or different
   */
  public static final String OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE =
      "otel.exporter.otlp.metrics.temporality.preference";

  /**
   * Default histogram aggregation to be used for all histograms: Select one of the below <br>
   * 1. base2_exponential_bucket_histogram <br>
   * 2. explicit_bucket_histogram
   */
  public static final String OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION =
      "otel.exporter.otlp.metrics.default.histogram.aggregation";

  /**
   * Max scale for base2_exponential_bucket_histogram
   */
  public static final String OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION_MAX_SCALE =
      "otel.exporter.otlp.metrics.default.histogram.aggregation.max.scale";

  /**
   * Max buckets for base2_exponential_bucket_histogram
   */
  public static final String OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION_MAX_BUCKETS =
      "otel.exporter.otlp.metrics.default.histogram.aggregation.max.buckets";

  private final String serviceName;
  private final String metricPrefix;
  /** reusing tehuti's MetricConfig */
  private final MetricConfig tehutiMetricConfig;

  /** Below are the configs for OpenTelemetry metrics */

  /** Feature flag to use OpenTelemetry instrumentation for metrics or not */
  private final boolean emitOTelMetrics;

  /** extra configs for OpenTelemetry. Supports 2 exporter currently <br>
   * 1. {@link MetricExporter} for exporting to Http/Grpc endpoint. More details are supported via configs,
   *    check {@link Builder#extractAndSetOtelConfigs} and {@link VeniceOpenTelemetryMetricsRepository#getOtlpHttpMetricExporter}<br>
   * 2. {@link VeniceOpenTelemetryMetricsRepository.LogBasedMetricExporter} for debug purposes
   */
  private final boolean exportOtelMetricsToEndpoint;
  private final boolean exportOtelMetricsToLog;

  /**
   * protocol for OpenTelemetry exporter. supports
   * 1. {@link OtlpConfigUtil#PROTOCOL_HTTP_PROTOBUF}  => "http/protobuf"
   * 2. {@link OtlpConfigUtil#PROTOCOL_GRPC}  => "grpc"
   */
  private final String otelExportProtocol;

  /** endpoint to export OpenTelemetry Metrics to */
  private final String otelEndpoint;

  /** Headers to be passed while creating OpenTelemetry exporter */
  private final Map<String, String> otelHeaders;

  /** Metric naming conventions for OpenTelemetry metrics */
  private final VeniceOpenTelemetryMetricNamingFormat metricNamingFormat;

  /** Aggregation Temporality selector to export only the delta or cumulate or different */
  private final AggregationTemporalitySelector otelAggregationTemporalitySelector;

  /** Default histogram aggregation to be used for all histograms: Select exponential or explicit bucket histogram */
  private final DefaultAggregationSelector otelHistogramAggregationSelector;

  private VeniceMetricsConfig(Builder builder) {
    this.serviceName = builder.serviceName;
    this.metricPrefix = builder.metricPrefix;
    this.emitOTelMetrics = builder.emitOtelMetrics;
    this.exportOtelMetricsToEndpoint = builder.exportOtelMetricsToEndpoint;
    this.otelExportProtocol = builder.otelExportProtocol;
    this.otelEndpoint = builder.otelEndpoint;
    this.otelHeaders = builder.otelHeaders;
    this.exportOtelMetricsToLog = builder.exportOtelMetricsToLog;
    this.metricNamingFormat = builder.metricNamingFormat;
    this.otelAggregationTemporalitySelector = builder.otelAggregationTemporalitySelector;
    this.otelHistogramAggregationSelector = builder.otelHistogramAggregationSelector;
    this.tehutiMetricConfig = builder.tehutiMetricConfig;
  }

  public static class Builder {
    private String serviceName = "default_service";
    private String metricPrefix = null;
    private boolean emitOtelMetrics = false;
    private boolean exportOtelMetricsToEndpoint = false;
    private String otelExportProtocol = OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF;
    private String otelEndpoint = null;
    Map<String, String> otelHeaders = new HashMap<>();
    private boolean exportOtelMetricsToLog = false;
    private VeniceOpenTelemetryMetricNamingFormat metricNamingFormat = VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE;
    private AggregationTemporalitySelector otelAggregationTemporalitySelector =
        AggregationTemporalitySelector.deltaPreferred();
    DefaultAggregationSelector otelHistogramAggregationSelector = null;
    private MetricConfig tehutiMetricConfig = null;

    public Builder setServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public Builder setMetricPrefix(String metricPrefix) {
      this.metricPrefix = metricPrefix;
      return this;
    }

    public Builder setEmitOtelMetrics(boolean emitOtelMetrics) {
      this.emitOtelMetrics = emitOtelMetrics;
      return this;
    }

    public Builder setExportOtelMetricsToEndpoint(boolean exportOtelMetricsToEndpoint) {
      this.exportOtelMetricsToEndpoint = exportOtelMetricsToEndpoint;
      return this;
    }

    public Builder setOtelExportProtocol(String otelExportProtocol) {
      this.otelExportProtocol = otelExportProtocol;
      return this;
    }

    public Builder setOtelEndpoint(String otelEndpoint) {
      this.otelEndpoint = otelEndpoint;
      return this;
    }

    public Builder setExportOtelMetricsToLog(boolean exportOtelMetricsToLog) {
      this.exportOtelMetricsToLog = exportOtelMetricsToLog;
      return this;
    }

    public Builder setMetricNamingFormat(VeniceOpenTelemetryMetricNamingFormat metricNamingFormat) {
      this.metricNamingFormat = metricNamingFormat;
      return this;
    }

    public Builder setOtelAggregationTemporalitySelector(
        AggregationTemporalitySelector otelAggregationTemporalitySelector) {
      this.otelAggregationTemporalitySelector = otelAggregationTemporalitySelector;
      return this;
    }

    public Builder setOtelHistogramAggregationSelector(DefaultAggregationSelector otelHistogramAggregationSelector) {
      this.otelHistogramAggregationSelector = otelHistogramAggregationSelector;
      return this;
    }

    /**
     * Extract and set otel configs
     */
    public Builder extractAndSetOtelConfigs(Map<String, String> configs) {
      String configValue;
      if ((configValue = configs.get(OTEL_VENICE_ENABLED)) != null) {
        setEmitOtelMetrics(Boolean.parseBoolean(configValue));
      }

      if ((configValue = configs.get(OTEL_VENICE_EXPORT_TO_LOG)) != null) {
        setExportOtelMetricsToLog(Boolean.parseBoolean(configValue));
      }

      if ((configValue = configs.get(OTEL_VENICE_EXPORT_TO_ENDPOINT)) != null) {
        setExportOtelMetricsToEndpoint(Boolean.parseBoolean(configValue));
      }

      if ((configValue = configs.get(OTEL_EXPORTER_OTLP_METRICS_PROTOCOL)) != null) {
        setOtelExportProtocol(configValue);
      }

      if ((configValue = configs.get(OTEL_VENICE_METRICS_NAMING_FORMAT)) != null) {
        setMetricNamingFormat(VeniceOpenTelemetryMetricNamingFormat.valueOf(configValue.toUpperCase(Locale.ROOT)));
      }

      if ((configValue = configs.get(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT)) != null) {
        // validate endpoint: TODO
        setOtelEndpoint(configValue);
      }

      /**
       * Headers are passed as key=value pairs separated by '='
       * Multiple headers are separated by ','
       *
       * Currently supporting 1 header
       */
      if ((configValue = configs.get(OTEL_EXPORTER_OTLP_METRICS_HEADERS)) != null) {
        String[] headers = configValue.split("=");
        otelHeaders.put(headers[0], headers[1]);
      }

      if ((configValue = configs.get(OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE)) != null) {
        switch (configValue.toLowerCase(Locale.ROOT)) {
          case "cumulative":
            setOtelAggregationTemporalitySelector(AggregationTemporalitySelector.alwaysCumulative());
            break;
          case "delta":
            setOtelAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred());
            break;
          case "lowmemory":
            setOtelAggregationTemporalitySelector(AggregationTemporalitySelector.lowMemory());
            break;
          default:
            throw new IllegalArgumentException("Unrecognized aggregation temporality: " + configValue);
        }
      }

      if ((configValue = configs.get(OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION)) != null) {
        switch (configValue.toLowerCase(Locale.ROOT)) {
          case "base2_exponential_bucket_histogram":
            String maxScaleValue = configs.get(OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION_MAX_SCALE);
            String maxBucketValue = configs.get(OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION_MAX_BUCKETS);
            if (maxScaleValue != null && maxBucketValue != null) {
              int maxScale = Integer.parseInt(maxScaleValue);
              int maxBuckets = Integer.parseInt(maxBucketValue);
              setOtelHistogramAggregationSelector(
                  DefaultAggregationSelector.getDefault()
                      .with(
                          InstrumentType.HISTOGRAM,
                          Aggregation.base2ExponentialBucketHistogram(maxBuckets, maxScale)));
            } else {
              setOtelHistogramAggregationSelector(
                  DefaultAggregationSelector.getDefault()
                      .with(InstrumentType.HISTOGRAM, Aggregation.base2ExponentialBucketHistogram()));
            }
            break;

          case "explicit_bucket_histogram":
            setOtelHistogramAggregationSelector(
                DefaultAggregationSelector.getDefault()
                    .with(InstrumentType.HISTOGRAM, Aggregation.explicitBucketHistogram()));
            break;

          default:
            throw new IllegalArgumentException("Unrecognized default histogram aggregation: " + configValue);
        }
      }

      // todo: add more configs
      // "otel.exporter.otlp.metrics.compression"
      // "otel.exporter.otlp.metrics.timeout"
      return this;
    }

    public Builder setTehutiMetricConfig(MetricConfig tehutiMetricConfig) {
      this.tehutiMetricConfig = tehutiMetricConfig;
      return this;
    }

    // Validate required fields before building
    private void checkAndSetDefaults() {
      if (tehutiMetricConfig == null) {
        setTehutiMetricConfig(new MetricConfig());
      }

      if (metricPrefix == null) {
        LOGGER.warn("metricPrefix is not set. Defaulting to empty string");
        setMetricPrefix("");
      }

      if (emitOtelMetrics) {
        if (exportOtelMetricsToEndpoint) {
          if (otelEndpoint == null) {
            throw new IllegalArgumentException("endpoint is required to configure OpenTelemetry metrics export");
          }

        } else {
          LOGGER.warn("OpenTelemetry metrics are enabled but no endpoint is configured to export metrics");
        }
      } else {
        LOGGER.warn("OpenTelemetry metrics are disabled");
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

  public boolean emitOtelMetrics() {
    return emitOTelMetrics;
  }

  public boolean exportOtelMetricsToEndpoint() {
    return exportOtelMetricsToEndpoint;
  }

  public String getOtelExportProtocol() {
    return otelExportProtocol;
  }

  public String getOtelEndpoint() {
    return otelEndpoint;
  }

  public boolean exportOtelMetricsToLog() {
    return exportOtelMetricsToLog;
  }

  public Map<String, String> getOtelHeaders() {
    return otelHeaders;
  }

  public VeniceOpenTelemetryMetricNamingFormat getMetricNamingFormat() {
    return metricNamingFormat;
  }

  public AggregationTemporalitySelector getOtelAggregationTemporalitySelector() {
    return otelAggregationTemporalitySelector;
  }

  public DefaultAggregationSelector getOtelHistogramAggregationSelector() {
    return otelHistogramAggregationSelector;
  }

  public MetricConfig getTehutiMetricConfig() {
    return tehutiMetricConfig;
  }

  @Override
  public String toString() {
    return "VeniceMetricsConfig{" + "serviceName='" + serviceName + '\'' + ", metricPrefix='" + metricPrefix + '\''
        + ", emitOTelMetrics=" + emitOTelMetrics + ", exportOtelMetricsToEndpoint=" + exportOtelMetricsToEndpoint
        + ", otelExportProtocol='" + otelExportProtocol + '\'' + ", otelEndpoint='" + otelEndpoint + '\''
        + ", otelHeaders=" + otelHeaders + ", exportOtelMetricsToLog=" + exportOtelMetricsToLog
        + ", metricNamingFormat=" + metricNamingFormat + ", otelAggregationTemporalitySelector="
        + otelAggregationTemporalitySelector + ", otelHistogramAggregationSelector=" + otelHistogramAggregationSelector
        + ", tehutiMetricConfig=" + tehutiMetricConfig + '}';
  }
}
