package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.transformMetricName;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.validateMetricName;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceOpenTelemetryMetricsRepository {
  private static final Logger LOGGER = LogManager.getLogger(VeniceOpenTelemetryMetricsRepository.class);
  private SdkMeterProvider sdkMeterProvider = null;
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricNamingFormat metricFormat;
  private Meter meter;

  private String metricPrefix;

  /** Below Maps are to create only one metric per name and type: Venice code will try to initialize the same metric multiple times as it will get
   * called from per store path and per request type path. This will ensure that we only have one metric per name and
   * use dimensions to differentiate between them.
   */
  private final VeniceConcurrentHashMap<String, DoubleHistogram> histogramMap = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, LongCounter> counterMap = new VeniceConcurrentHashMap<>();

  MetricExporter getOtlpHttpMetricExporter(VeniceMetricsConfig metricsConfig) {
    OtlpHttpMetricExporterBuilder exporterBuilder =
        OtlpHttpMetricExporter.builder().setEndpoint(metricsConfig.getOtelEndpoint());
    for (Map.Entry<String, String> entry: metricsConfig.getOtelHeaders().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      exporterBuilder.addHeader(key, value);
    }
    if (metricsConfig.getOtelAggregationTemporalitySelector() != null) {
      exporterBuilder.setAggregationTemporalitySelector(metricsConfig.getOtelAggregationTemporalitySelector());
    }
    return exporterBuilder.build();
  }

  /**
   * Setting Exponential Histogram aggregation for {@link MetricType#HISTOGRAM} by looping through all
   * the metric entities set for this service to registering the view with exponential histogram aggregation for
   * all the {@link MetricType#HISTOGRAM} metrics.
   *
   * There is a limitation in opentelemetry sdk to configure different histogram aggregation for different
   * instruments, so {@link OtlpHttpMetricExporterBuilder#setDefaultAggregationSelector} to enable exponential
   * histogram aggregation is not used here to not convert the histograms of type {@link MetricType#MIN_MAX_COUNT_SUM_AGGREGATIONS}
   * to exponential histograms to be able to follow explict boundaries.
   *
   * If the metric entities are empty, it will throw an exception. Failing fast here as
   * 1. If we configure exponential histogram aggregation for every histogram: it could lead to increased memory usage
   * 2. If we don't configure exponential histogram aggregation for every histogram: it could lead to observability miss
   */
  private void setExponentialHistogramAggregation(SdkMeterProviderBuilder builder, VeniceMetricsConfig metricsConfig) {
    List<String> metricNames = new ArrayList<>();

    Collection<MetricEntity> metricEntities = metricsConfig.getMetricEntities();
    if (metricEntities == null || metricsConfig.getMetricEntities().isEmpty()) {
      throw new IllegalArgumentException(
          "metricEntities cannot be empty if exponential Histogram is enabled, List all the metrics used in this service using setMetricEntities method");
    }

    for (MetricEntity metricEntity: metricsConfig.getMetricEntities()) {
      if (metricEntity.getMetricType() == MetricType.HISTOGRAM) {
        metricNames.add(getFullMetricName(getMetricPrefix(), metricEntity.getMetricName()));
      }
    }

    // Register views for all MetricType.HISTOGRAM metrics to be aggregated/exported as exponential histograms
    for (String metricName: metricNames) {
      InstrumentSelector selector =
          InstrumentSelector.builder().setType(InstrumentType.HISTOGRAM).setName(metricName).build();

      builder.registerView(
          selector,
          View.builder()
              .setAggregation(
                  Aggregation.base2ExponentialBucketHistogram(
                      metricsConfig.getOtelExponentialHistogramMaxBuckets(),
                      metricsConfig.getOtelExponentialHistogramMaxScale()))
              .build());
    }
  }

  public VeniceOpenTelemetryMetricsRepository(VeniceMetricsConfig metricsConfig) {
    emitOpenTelemetryMetrics = metricsConfig.emitOtelMetrics();
    metricFormat = metricsConfig.getMetricNamingFormat();
    if (!emitOpenTelemetryMetrics) {
      LOGGER.info("OpenTelemetry metrics are disabled");
      return;
    }
    LOGGER.info(
        "OpenTelemetry initialization for {} started with config: {}",
        metricsConfig.getServiceName(),
        metricsConfig.toString());
    this.metricPrefix = "venice." + metricsConfig.getMetricPrefix();
    validateMetricName(this.metricPrefix);
    try {
      SdkMeterProviderBuilder builder = SdkMeterProvider.builder();
      if (metricsConfig.exportOtelMetricsToEndpoint()) {
        MetricExporter httpExporter = getOtlpHttpMetricExporter(metricsConfig);
        builder.registerMetricReader(
            PeriodicMetricReader.builder(httpExporter)
                .setInterval(metricsConfig.getExportOtelMetricsIntervalInSeconds(), TimeUnit.SECONDS)
                .build());
      }
      if (metricsConfig.exportOtelMetricsToLog()) {
        // internal to test: Disabled by default
        builder.registerMetricReader(
            PeriodicMetricReader.builder(new LogBasedMetricExporter(metricsConfig))
                .setInterval(metricsConfig.getExportOtelMetricsIntervalInSeconds(), TimeUnit.SECONDS)
                .build());
      }

      if (metricsConfig.useOtelExponentialHistogram()) {
        setExponentialHistogramAggregation(builder, metricsConfig);
      }

      builder.setResource(Resource.empty());
      sdkMeterProvider = builder.build();

      // Register MeterProvider with the OpenTelemetry instance
      OpenTelemetry openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();

      this.meter = openTelemetry.getMeter(transformMetricName(getMetricPrefix(), metricFormat));
      LOGGER.info(
          "OpenTelemetry initialization for {} completed with config: {}",
          metricsConfig.getServiceName(),
          metricsConfig.toString());
    } catch (Exception e) {
      String err = "OpenTelemetry initialization for " + metricsConfig.getServiceName() + " failed with config: "
          + metricsConfig.toString();
      LOGGER.error(err, e);
      throw new VeniceException(err, e);
    }
  }

  String getFullMetricName(String metricPrefix, String name) {
    String fullMetricName = metricPrefix + "." + name;
    validateMetricName(fullMetricName);
    return transformMetricName(fullMetricName, metricFormat);
  }

  private String getMetricPrefix() {
    return metricPrefix;
  }

  public DoubleHistogram createHistogram(MetricEntity metricEntity) {
    if (!emitOpenTelemetryMetrics) {
      return null;
    }
    return histogramMap.computeIfAbsent(metricEntity.getMetricName(), key -> {
      String fullMetricName = getFullMetricName(getMetricPrefix(), metricEntity.getMetricName());
      DoubleHistogramBuilder builder = meter.histogramBuilder(fullMetricName)
          .setUnit(metricEntity.getUnit().name())
          .setDescription(metricEntity.getDescription());
      if (metricEntity.getMetricType() == MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS) {
        // No buckets needed to get only min/max/count/sum aggregations
        builder.setExplicitBucketBoundariesAdvice(new ArrayList<>());
      }
      return builder.build();
    });
  }

  public LongCounter createCounter(MetricEntity metricEntity) {
    if (!emitOpenTelemetryMetrics) {
      return null;
    }
    return counterMap.computeIfAbsent(metricEntity.getMetricName(), key -> {
      String fullMetricName = getFullMetricName(getMetricPrefix(), metricEntity.getMetricName());
      LongCounterBuilder builder = meter.counterBuilder(fullMetricName)
          .setUnit(metricEntity.getUnit().name())
          .setDescription(metricEntity.getDescription());
      return builder.build();
    });
  }

  public Object createInstrument(MetricEntity metricEntity) {
    MetricType metricType = metricEntity.getMetricType();
    switch (metricType) {
      case HISTOGRAM:
      case MIN_MAX_COUNT_SUM_AGGREGATIONS:
        return createHistogram(metricEntity);

      case COUNTER:
        return createCounter(metricEntity);

      default:
        throw new VeniceException("Unknown metric type: " + metricType);
    }
  }

  public void close() {
    if (sdkMeterProvider != null) {
      sdkMeterProvider.shutdown();
      sdkMeterProvider = null;
    }
  }

  class LogBasedMetricExporter implements MetricExporter {
    VeniceMetricsConfig metricsConfig;

    LogBasedMetricExporter(VeniceMetricsConfig metricsConfig) {
      this.metricsConfig = metricsConfig;
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
      return metricsConfig.getOtelAggregationTemporalitySelector().getAggregationTemporality(instrumentType);
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
      LOGGER.info("Logging OpenTelemetry metrics for debug purpose: {}", Arrays.toString(metrics.toArray()));
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }
  }

  /** for testing purposes */
  SdkMeterProvider getSdkMeterProvider() {
    return sdkMeterProvider;
  }

  /** for testing purposes */
  Meter getMeter() {
    return meter;
  }
}
