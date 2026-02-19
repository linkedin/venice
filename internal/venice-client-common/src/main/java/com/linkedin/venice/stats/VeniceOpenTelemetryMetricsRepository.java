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
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
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
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceOpenTelemetryMetricsRepository {
  private static final Logger LOGGER = LogManager.getLogger(VeniceOpenTelemetryMetricsRepository.class);
  public static final RedundantLogFilter REDUNDANT_LOG_FILTER = RedundantLogFilter.getRedundantLogFilter();
  public static final String DEFAULT_METRIC_PREFIX = "venice.";
  private final VeniceMetricsConfig metricsConfig;

  /** OpenTelemetry instance: Either created or retrieved from GlobalOpenTelemetry set by the application */
  private final OpenTelemetry openTelemetry;
  /** SdkMeterProvider that is used to create the OpenTelemetry instance */
  private SdkMeterProvider sdkMeterProvider = null;

  private final boolean emitOpenTelemetryMetrics;
  private final boolean emitTehutiMetrics;
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
    emitTehutiMetrics = metricsConfig.emitTehutiMetrics();
    metricFormat = metricsConfig.getMetricNamingFormat();
    this.metricPrefix = metricsConfig.getMetricPrefix();
    validateMetricName(getMetricPrefix());
    if (!emitOpenTelemetryMetrics) {
      LOGGER.info("OpenTelemetry metrics are disabled");
      openTelemetry = null;
      return;
    }
    this.openTelemetry = initializeOpenTelemetry(metricsConfig);
    this.meter = openTelemetry.getMeter(transformMetricName(getMetricPrefix(), metricFormat));
    this.recordFailureMetric = MetricEntityStateBase.create(
        CommonMetricsEntity.METRIC_RECORD_FAILURE.getMetricEntity(),
        this,
        Collections.EMPTY_MAP,
        Attributes.empty());
  }

  /**
   * Private constructor for creating a child instance that shares the same OpenTelemetry SDK
   * but uses a different metric prefix. This avoids reinitializing the OpenTelemetry SDK.
   *
   * <p>When adding new fields to this class, you MUST update this constructor to properly
   * initialize the new field. Fields fall into three categories:</p>
   * <ol>
   *   <li><b>Shared from parent:</b> Fields that should be copied from parent (e.g., metricsConfig,
   *       openTelemetry, emitOpenTelemetryMetrics)</li>
   *   <li><b>Child-specific:</b> Fields that should have new values for child (e.g., metricPrefix,
   *       meter, instrument maps)</li>
   *   <li><b>Ownership flags:</b> Fields indicating resource ownership (e.g., sdkMeterProvider
   *       should be null for child)</li>
   * </ol>
   * <p>A unit test {@code testCloneWithNewMetricPrefixCopiesAllRequiredFields} in
   * {@code VeniceOpenTelemetryMetricsRepositoryTest} uses reflection to verify all fields are
   * properly initialized.</p>
   *
   * @param parent The parent repository to share OpenTelemetry instance from
   * @param newMetricPrefix The new metric prefix to use for this child instance
   */
  private VeniceOpenTelemetryMetricsRepository(VeniceOpenTelemetryMetricsRepository parent, String newMetricPrefix) {
    this.metricsConfig = parent.metricsConfig;
    this.emitOpenTelemetryMetrics = parent.emitOpenTelemetryMetrics;
    this.emitTehutiMetrics = parent.emitTehutiMetrics;
    this.metricFormat = parent.metricFormat;
    this.metricPrefix = newMetricPrefix;
    this.openTelemetry = parent.openTelemetry;
    this.sdkMeterProvider = null; // Child does not own the provider
    validateMetricName(getMetricPrefix());

    if (emitOpenTelemetryMetrics && openTelemetry != null) {
      // Create a new Meter with the new prefix
      this.meter = openTelemetry.getMeter(transformMetricName(getMetricPrefix(), metricFormat));
      this.recordFailureMetric = MetricEntityStateBase.create(
          CommonMetricsEntity.METRIC_RECORD_FAILURE.getMetricEntity(),
          this,
          Collections.EMPTY_MAP,
          Attributes.empty());
    }
    LOGGER.info("Created child VeniceOpenTelemetryMetricsRepository with metric prefix: {}", newMetricPrefix);
  }

  /**
   * Creates a new repository that shares the same OpenTelemetry SDK instance
   * but uses a different metric prefix. This is useful for emitting metrics with a
   * different prefix (e.g., "participant_store_client") without reinitializing OpenTelemetry.
   *
   * @param newMetricPrefix The metric prefix to use for the child repository
   * @return A new VeniceOpenTelemetryMetricsRepository instance with the specified prefix
   */
  public VeniceOpenTelemetryMetricsRepository cloneWithNewMetricPrefix(String newMetricPrefix) {
    return new VeniceOpenTelemetryMetricsRepository(this, newMetricPrefix);
  }

  private OpenTelemetry initializeOpenTelemetry(VeniceMetricsConfig metricsConfig) {
    OpenTelemetry otel;
    if (metricsConfig.useOpenTelemetryInitializedByApplication()) {
      LOGGER.info("Using globally initialized OpenTelemetry for {}", metricsConfig.getServiceName());
      otel = GlobalOpenTelemetry.get();
      if (otel == null || otel.getMeterProvider() == null || otel.getMeterProvider().equals(MeterProvider.noop())) {
        LOGGER.warn(
            "Global OpenTelemetry is not initialized properly. Falling back to local initialization for {}",
            metricsConfig.getServiceName());
      } else {
        LOGGER.info("Successfully obtained globally initialized OpenTelemetry for {}", metricsConfig.getServiceName());
        return otel;
      }
    }

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
      otel = OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();
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

    return otel;
  }

  /**
   * To create only one metric per name and type: Venice code will try to initialize the same metric multiple times as
   * it will get called from per store path, per request type path, etc. This will ensure that we only have one metric
   * per name and use dimensions to differentiate between them.
   */
  private final VeniceConcurrentHashMap<String, DoubleHistogram> histogramMap = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, LongCounter> counterMap = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, LongUpDownCounter> upDownCounterMap = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, LongGauge> gaugeMap = new VeniceConcurrentHashMap<>();

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

  public DoubleHistogram createDoubleHistogram(MetricEntity metricEntity) {
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

  public LongCounter createLongCounter(MetricEntity metricEntity) {
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

  public LongUpDownCounter createLongUpDownCounter(MetricEntity metricEntity) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    return upDownCounterMap.computeIfAbsent(metricEntity.getMetricName(), key -> {
      String fullMetricName = getFullMetricName(metricEntity);
      LongUpDownCounterBuilder builder = meter.upDownCounterBuilder(fullMetricName)
          .setUnit(metricEntity.getUnit().name())
          .setDescription(getMetricDescription(metricEntity, metricsConfig));
      return builder.build();
    });
  }

  public LongGauge createLongGuage(MetricEntity metricEntity) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    return gaugeMap.computeIfAbsent(metricEntity.getMetricName(), key -> {
      String fullMetricName = getFullMetricName(metricEntity);
      LongGaugeBuilder builder = meter.gaugeBuilder(fullMetricName)
          .setUnit(metricEntity.getUnit().name())
          .setDescription(getMetricDescription(metricEntity, metricsConfig))
          .ofLongs();
      return builder.build();
    });
  }

  /**
   * Asynchronous gauge that will call the callback during metrics collection.
   * This is useful for metrics that are not updated frequently or require expensive computation.
   * Multiple callers can register callbacks for the same metric name (e.g., different stores);
   * all callbacks are invoked during each collection cycle.
   */
  public ObservableLongGauge createAsyncLongGauge(
      MetricEntity metricEntity,
      @Nonnull LongSupplier asyncCallback,
      @Nonnull Attributes attributes) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    return meter.gaugeBuilder(getFullMetricName(metricEntity))
        .setUnit(metricEntity.getUnit().name())
        .setDescription(getMetricDescription(metricEntity, metricsConfig))
        .ofLongs()
        .buildWithCallback(measurement -> {
          long v;
          try {
            v = asyncCallback.getAsLong();
          } catch (Exception e) {
            recordFailureMetric(metricEntity, e);
            return;
          }
          measurement.record(v, attributes);
        });
  }

  public Object createInstrument(MetricEntity metricEntity, LongSupplier asyncCallback, Attributes attributes) {
    MetricType metricType = metricEntity.getMetricType();
    switch (metricType) {
      case HISTOGRAM:
      case MIN_MAX_COUNT_SUM_AGGREGATIONS:
        return createDoubleHistogram(metricEntity);

      case COUNTER:
        return createLongCounter(metricEntity);

      case UP_DOWN_COUNTER:
        return createLongUpDownCounter(metricEntity);

      case GAUGE:
        return createLongGuage(metricEntity);

      case ASYNC_GAUGE:
        return createAsyncLongGauge(metricEntity, asyncCallback, attributes);

      case ASYNC_COUNTER_FOR_HIGH_PERF_CASES:
      case ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES:
        /**
         * Observable Counter/UpDownCounter is registered separately after the MetricEntityState is constructed
         * because the callback needs access to the MetricEntityState's subclass's metricAttributesData map.
         * See registerObservableLongCounter and registerObservableLongUpDownCounter methods.
         */
        return null;

      default:
        throw new VeniceException("Unknown metric type: " + metricType);
    }
  }

  @VisibleForTesting
  public Object createInstrument(MetricEntity metricEntity) {
    return createInstrument(metricEntity, null, null);
  }

  /**
   * Registers an Observable Long Counter that reads accumulated values from a callback.
   * This method should be called after the MetricEntityState is fully constructed,
   * as the callback needs access to the metricAttributesData map.
   *
   * <p>For {@link MetricType#ASYNC_COUNTER_FOR_HIGH_PERF_CASES} metrics, the callback is invoked during
   * OpenTelemetry's metric collection cycle. The callback should iterate over all
   * accumulated values and report them via the provided {@link ObservableLongMeasurement}.
   *
   * @param metricEntity the metric entity definition
   * @param reportCallback callback that reports all accumulated values to the measurement
   * @return the created ObservableLongCounter, or null if OTel metrics are disabled
   */
  public ObservableLongCounter registerObservableLongCounter(
      MetricEntity metricEntity,
      @Nonnull Consumer<ObservableLongMeasurement> reportCallback) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    if (metricEntity.getMetricType() != MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES) {
      throw new IllegalArgumentException(
          "registerObservableLongCounter should only be called for ASYNC_COUNTER_FOR_HIGH_PERF_CASES metrics, but got: "
              + metricEntity.getMetricType() + " for metric: " + metricEntity.getMetricName());
    }
    return meter.counterBuilder(getFullMetricName(metricEntity))
        .setUnit(metricEntity.getUnit().name())
        .setDescription(getMetricDescription(metricEntity, metricsConfig))
        .buildWithCallback(reportCallback);
  }

  /**
   * Registers an Observable Long UpDownCounter that reads accumulated values from a callback.
   * This method should be called after the MetricEntityState is fully constructed,
   * as the callback needs access to the metricAttributesData map.
   *
   * <p>For {@link MetricType#ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES} metrics, the callback is invoked during
   * OpenTelemetry's metric collection cycle. The callback should iterate over all
   * accumulated values and report them via the provided {@link ObservableLongMeasurement}.
   * Unlike ASYNC_COUNTER_FOR_HIGH_PERF_CASES, this supports both positive and negative values.
   *
   * @param metricEntity the metric entity definition
   * @param reportCallback callback that reports all accumulated values to the measurement
   * @return the created ObservableLongUpDownCounter, or null if OTel metrics are disabled
   */
  public ObservableLongUpDownCounter registerObservableLongUpDownCounter(
      MetricEntity metricEntity,
      @Nonnull Consumer<ObservableLongMeasurement> reportCallback) {
    if (!emitOpenTelemetryMetrics()) {
      return null;
    }
    if (metricEntity.getMetricType() != MetricType.ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES) {
      throw new IllegalArgumentException(
          "registerObservableLongUpDownCounter should only be called for ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES metrics, but got: "
              + metricEntity.getMetricType() + " for metric: " + metricEntity.getMetricName());
    }
    return meter.upDownCounterBuilder(getFullMetricName(metricEntity))
        .setUnit(metricEntity.getUnit().name())
        .setDescription(getMetricDescription(metricEntity, metricsConfig))
        .buildWithCallback(reportCallback);
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

  public void recordFailureMetric(MetricEntity metricEntity, Exception e) {
    getRecordFailureMetric().record(1);
    if (!REDUNDANT_LOG_FILTER.isRedundantLog(e.getMessage())) {
      LOGGER.error("Error recording metric {} with exception: ", metricEntity.getMetricName(), e);
    }
  }

  public void recordFailureMetric(MetricEntity metricEntity, String error) {
    getRecordFailureMetric().record(1);
    if (!REDUNDANT_LOG_FILTER.isRedundantLog(error)) {
      LOGGER.error("Error recording metric {} with error: {}", metricEntity.getMetricName(), error);
    }
  }

  public boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  public boolean emitTehutiMetrics() {
    return emitTehutiMetrics;
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
      this.metricEntity =
          MetricEntity.createWithNoDimensions(this.name().toLowerCase(), metricType, unit, description, "internal");
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
