package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.transformMetricName;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.validateMetricName;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
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
import io.tehuti.utils.RedundantLogFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceOpenTelemetryMetricsRepository {
  private static final Logger LOGGER = LogManager.getLogger(VeniceOpenTelemetryMetricsRepository.class);
  public static final RedundantLogFilter REDUNDANT_LOG_FILTER = RedundantLogFilter.getRedundantLogFilter();
  public static final String DEFAULT_METRIC_PREFIX = "venice.";
  private final VeniceMetricsConfig metricsConfig;
  private SdkMeterProvider sdkMeterProvider = null;
  private final OpenTelemetry openTelemetry;
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricNamingFormat metricFormat;
  private Meter meter;
  private String metricPrefix;
  /**
   * This metric is used to track the number of failures while recording metrics.
   * Currently used in {@link com.linkedin.venice.stats.metrics.MetricEntityStateGeneric}
   * to record if the dimensions passed in are invalid.
   */
  private MetricEntityStateBase recordFailureMetric;

  public VeniceOpenTelemetryMetricsRepository(VeniceMetricsConfig metricsConfig) {
    this.metricsConfig = metricsConfig;
    emitOpenTelemetryMetrics = metricsConfig.emitOtelMetrics();
    metricFormat = metricsConfig.getMetricNamingFormat();
    if (!emitOpenTelemetryMetrics) {
      LOGGER.info("OpenTelemetry metrics are disabled");
      openTelemetry = null;
      return;
    }
    this.metricPrefix = metricsConfig.getMetricPrefix();
    validateMetricName(getMetricPrefix());
    if (metricsConfig.useOpenTelemetryInitializedByApplication()) {
      LOGGER.info("Using globally initialized OpenTelemetry for {}", metricsConfig.getServiceName());
      openTelemetry = GlobalOpenTelemetry.get();
      if (openTelemetry == null || openTelemetry.getMeterProvider() == null
          || openTelemetry.getMeterProvider().equals(MeterProvider.noop())) {
        // Fail fast if no global OpenTelemetry instance is initialized to avoid silent metric loss.
        // When disabled, each Venice component will initialize its own OpenTelemetry instance,
        // which can lead to multiple instances in the same application, especially when used as
        // a library (common for Venice clients) when multiple such libraries initialized.
        throw new VeniceException(
            "OpenTelemetry is not initialized globally by the application: disable the configuration or "
                + "initialize OpenTelemetry in the application before initializing venice");
      }
    } else {
      LOGGER.info(
          "OpenTelemetry initialization for {} started with config: {}",
          metricsConfig.getServiceName(),
          metricsConfig.toString());
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

        if (metricsConfig.getOtelAdditionalMetricsReader() != null) {
          // additional metrics reader apart from the above. For instance,
          // an in-memory metric reader can be passed in for testing purposes.
          builder.registerMetricReader(metricsConfig.getOtelAdditionalMetricsReader());
        }

        if (metricsConfig.useOtelExponentialHistogram()) {
          setExponentialHistogramAggregation(builder, metricsConfig);
        }

        // Set resource to empty to avoid adding any default resource attributes. The receiver
        // pipeline can choose to add the respective resource attributes if needed.
        builder.setResource(Resource.empty());

        sdkMeterProvider = builder.build();

        // Register MeterProvider with the OpenTelemetry instance
        openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();
        LOGGER.info(
            "OpenTelemetry initialization for {} completed with config: {}",
            metricsConfig.getServiceName(),
            metricsConfig);
      } catch (Exception e) {
        String err = "OpenTelemetry initialization for " + metricsConfig.getServiceName() + " failed with config: "
            + metricsConfig;
        LOGGER.error(err, e);
        throw new VeniceException(err, e);
      }
    }

    this.meter = openTelemetry.getMeter(transformMetricName(getMetricPrefix(), metricFormat));
    this.recordFailureMetric = MetricEntityStateBase.create(
        CommonMetricsEntity.METRIC_RECORD_FAILURE.getMetricEntity(),
        this,
        Collections.EMPTY_MAP,
        Attributes.empty());
  }

  /**
   * To create only one metric per name and type: Venice code will try to initialize the same metric multiple times as
   * it will get called from per store path, per request type path, etc. This will ensure that we only have one metric
   * per name and use dimensions to differentiate between them.
   */
  private final VeniceConcurrentHashMap<String, DoubleHistogram> histogramMap = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, LongCounter> counterMap = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, DoubleGauge> gaugeMap = new VeniceConcurrentHashMap<>();

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
   * Setting Exponential Histogram aggregation for each {@link MetricType#HISTOGRAM} metric by looping through all
   * the metric entities set for this service using views.
   *
   * Because the OpenTelemetry SDK cannot currently assign different histogram aggregations to different histogram
   * instruments, we deliberately avoid {@link OtlpHttpMetricExporterBuilder#setDefaultAggregationSelector}. Using it
   * would also convert {@link MetricType#MIN_MAX_COUNT_SUM_AGGREGATIONS} to exponential histograms.
   *
   * If the metric entities are empty, it will throw an exception. Failing fast here as
   * 1. If we configure exponential histogram aggregation for every histogram: it could lead to increased memory usage
   * 2. If we don't configure exponential histogram aggregation for every histogram: it could lead to observability miss
   */
  private void setExponentialHistogramAggregation(SdkMeterProviderBuilder builder, VeniceMetricsConfig metricsConfig) {
    Set<String> uniqueHistogramMetricNames = new HashSet<>();

    Collection<MetricEntity> metricEntities = metricsConfig.getMetricEntities();
    if (metricEntities == null || metricsConfig.getMetricEntities().isEmpty()) {
      throw new IllegalArgumentException(
          "metricEntities cannot be empty if exponential Histogram is enabled, List all the metrics used in this service using setMetricEntities method");
    }

    for (MetricEntity metricEntity: metricsConfig.getMetricEntities()) {
      if (metricEntity.getMetricType() == MetricType.HISTOGRAM) {
        uniqueHistogramMetricNames.add(getFullMetricName(metricEntity));
      }
    }

    // Register views for all MetricType.HISTOGRAM metrics to be aggregated/exported as exponential histograms
    for (String metricName: uniqueHistogramMetricNames) {
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

  String getFullMetricName(MetricEntity metricEntity) {
    String fullMetricName = getMetricPrefix(metricEntity) + "." + metricEntity.getMetricName();
    validateMetricName(fullMetricName);
    return transformMetricName(fullMetricName, getMetricFormat());
  }

  static String createFullMetricPrefix(String metricPrefix) {
    return DEFAULT_METRIC_PREFIX + metricPrefix;
  }

  final String getMetricPrefix() {
    return createFullMetricPrefix(metricPrefix);
  }

  String getMetricPrefix(MetricEntity metricEntity) {
    return (metricEntity.getCustomMetricPrefix() == null
        ? getMetricPrefix()
        : createFullMetricPrefix(metricEntity.getCustomMetricPrefix()));
  }

  static String getMetricDescription(MetricEntity metricEntity, VeniceMetricsConfig metricsConfig) {
    String customDescription = metricsConfig.getOtelCustomDescriptionForHistogramMetrics();
    if (metricEntity.getMetricType() == MetricType.HISTOGRAM && customDescription != null
        && !customDescription.isEmpty()) {
      return customDescription;
    } else {
      return metricEntity.getDescription();
    }
  }

  public DoubleHistogram createHistogram(MetricEntity metricEntity) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    return histogramMap.computeIfAbsent(metricEntity.getMetricName(), key -> {
      String fullMetricName = getFullMetricName(metricEntity);
      DoubleHistogramBuilder builder = meter.histogramBuilder(fullMetricName)
          .setUnit(metricEntity.getUnit().name())
          .setDescription(getMetricDescription(metricEntity, metricsConfig));
      if (metricEntity.getMetricType() == MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS) {
        // No buckets needed to get only min/max/count/sum aggregations
        builder.setExplicitBucketBoundariesAdvice(new ArrayList<>()).setDescription(metricEntity.getDescription());
      }
      return builder.build();
    });
  }

  public LongCounter createCounter(MetricEntity metricEntity) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    return counterMap.computeIfAbsent(metricEntity.getMetricName(), key -> {
      String fullMetricName = getFullMetricName(metricEntity);
      LongCounterBuilder builder = meter.counterBuilder(fullMetricName)
          .setUnit(metricEntity.getUnit().name())
          .setDescription(getMetricDescription(metricEntity, metricsConfig));
      return builder.build();
    });
  }

  public DoubleGauge createGuage(MetricEntity metricEntity) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    return gaugeMap.computeIfAbsent(metricEntity.getMetricName(), key -> {
      String fullMetricName = getFullMetricName(metricEntity);
      DoubleGaugeBuilder builder = meter.gaugeBuilder(fullMetricName)
          .setUnit(metricEntity.getUnit().name())
          .setDescription(getMetricDescription(metricEntity, metricsConfig));
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
      case GAUGE:
        return createGuage(metricEntity);

      default:
        throw new VeniceException("Unknown metric type: " + metricType);
    }
  }

  public String getDimensionName(VeniceMetricsDimensions dimension) {
    return dimension.getDimensionName(getMetricFormat());
  }

  private void validateDimensionValuesAndThrow(
      MetricEntity metricEntity,
      VeniceMetricsDimensions dimension,
      String dimensionValue) {
    if (dimensionValue == null || dimensionValue.isEmpty()) {
      String errorLog = "Dimension value cannot be null or empty for key: " + dimension + " for metric: "
          + metricEntity.getMetricName();
      if (!REDUNDANT_LOG_FILTER.isRedundantLog(errorLog)) {
        LOGGER.error(errorLog);
      }
      throw new IllegalArgumentException(errorLog);
    }
  }

  private void validateDimensionValuesAndBuildAttributes(
      MetricEntity metricEntity,
      VeniceMetricsDimensions dimension,
      String dimensionValue,
      AttributesBuilder attributesBuilder) {
    validateDimensionValuesAndThrow(metricEntity, dimension, dimensionValue);
    attributesBuilder.put(getDimensionName(dimension), dimensionValue);
  }

  private AttributesBuilder createAttributesBuilderWithBaseAndCustomDimensions(
      MetricEntity metricEntity,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    // add common dimensions
    baseDimensionsMap.forEach(
        (key, value) -> validateDimensionValuesAndBuildAttributes(metricEntity, key, value, attributesBuilder));

    // add custom dimensions passed in by the user
    getMetricsConfig().getOtelCustomDimensionsMap().forEach(attributesBuilder::put);
    return attributesBuilder;
  }

  public Attributes createAttributes(
      MetricEntity metricEntity,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      VeniceDimensionInterface... additionalDimensionEnums) {
    AttributesBuilder attributesBuilder =
        createAttributesBuilderWithBaseAndCustomDimensions(metricEntity, baseDimensionsMap);

    // add additional dimensions passed in as type VeniceDimensionInterface
    for (VeniceDimensionInterface additionalDimensionEnum: additionalDimensionEnums) {
      validateDimensionValuesAndBuildAttributes(
          metricEntity,
          additionalDimensionEnum.getDimensionName(),
          additionalDimensionEnum.getDimensionValue(),
          attributesBuilder);
    }

    return attributesBuilder.build();
  }

  public Attributes createAttributes(
      MetricEntity metricEntity,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Map<VeniceMetricsDimensions, String> additionalDimensionsMap) {
    AttributesBuilder attributesBuilder =
        createAttributesBuilderWithBaseAndCustomDimensions(metricEntity, baseDimensionsMap);

    // add additional dimensions passed in as a map
    additionalDimensionsMap.forEach(
        (key, value) -> validateDimensionValuesAndBuildAttributes(metricEntity, key, value, attributesBuilder));

    return attributesBuilder.build();
  }

  public void close() {
    if (sdkMeterProvider != null) {
      sdkMeterProvider.shutdown();
      sdkMeterProvider = null;
    }
  }

  static class LogBasedMetricExporter implements MetricExporter {
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

  public void recordFailureMetric() {
    getRecordFailureMetric().record(1);
  }

  public boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  public VeniceMetricsConfig getMetricsConfig() {
    return metricsConfig;
  }

  public VeniceOpenTelemetryMetricNamingFormat getMetricFormat() {
    return metricFormat;
  }

  /**
   * List of generic metrics for Otel repository
   */
  private enum CommonMetricsEntity {
    METRIC_RECORD_FAILURE(MetricType.COUNTER, MetricUnit.NUMBER, "Count of all failures during metrics recording");

    private final MetricEntity metricEntity;

    CommonMetricsEntity(MetricType metricType, MetricUnit unit, String description) {
      this.metricEntity = MetricEntity.createInternalMetricEntityWithoutDimensions(
          this.name().toLowerCase(),
          metricType,
          unit,
          description,
          "internal");
    }

    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }

  @VisibleForTesting
  SdkMeterProvider getSdkMeterProvider() {
    return sdkMeterProvider;
  }

  @VisibleForTesting
  OpenTelemetry getOpenTelemetry() {
    return openTelemetry;
  }

  @VisibleForTesting
  Meter getMeter() {
    return meter;
  }

  @VisibleForTesting
  public MetricEntityStateBase getRecordFailureMetric() {
    return this.recordFailureMetric;
  }
}
