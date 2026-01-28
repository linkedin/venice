package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.tehuti.metrics.MeasurableStat;
import java.util.List;
import java.util.Map;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjLongConsumer;


/**
 * Abstract operational state of a non-async metric extended on top of {@link AsyncMetricEntityState}
 * to provide common functionality for non-async metrics like record() which is not supported for
 * async metrics.
 *
 * This abstract class should be extended by different MetricEntityStates like {@link MetricEntityStateBase} to
 * pre-create/cache the {@link Attributes} for different number/type of dimensions. check out the
 * classes extending this for more details. <br>
 */
public abstract class MetricEntityState extends AsyncMetricEntityState {
  private final boolean isObservableCounter;
  /** define both long and double consumer to avoid unnecessary conversions **/
  private final ObjDoubleConsumer<MetricAttributesData> otelDoubleRecordingStrategy;
  private final ObjLongConsumer<MetricAttributesData> otelLongRecordingStrategy;

  public MetricEntityState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        null,
        null);
    MetricType metricType = metricEntity.getMetricType();
    this.isObservableCounter = metricType.isObservableCounterType();
    this.otelDoubleRecordingStrategy = createOtelDoubleRecordingStrategy(metricType);
    this.otelLongRecordingStrategy = createOtelLongRecordingStrategy(metricType);
  }

  /**
   * Returns an iterable of all MetricAttributesData for observable counter reporting.
   * Subclasses must implement this to provide iteration over their specific EnumMap structure.
   * Returns null if metrics are not enabled or no data exists.
   */
  protected abstract Iterable<MetricAttributesData> getAllMetricAttributesData();

  /**
   * Registers the Observable Counter with the OTel repository if this metric uses async recording.
   * Supports both ASYNC_COUNTER_FOR_HIGH_PERF_CASES and ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES.
   * This must be called by subclasses after their constructor completes and the metricAttributesData map is initialized.
   */
  protected final void registerObservableCounterIfNeeded() {
    if (!isObservableCounter || !emitOpenTelemetryMetrics() || getOtelRepository() == null) {
      return;
    }
    switch (getMetricEntity().getMetricType()) {
      case ASYNC_COUNTER_FOR_HIGH_PERF_CASES:
        setOtelMetric(getOtelRepository().registerObservableLongCounter(getMetricEntity(), this::reportToMeasurement));
        break;
      case ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES:
        setOtelMetric(
            getOtelRepository().registerObservableLongUpDownCounter(getMetricEntity(), this::reportToMeasurement));
        break;
      default:
        throw new IllegalStateException(
            "Unexpected metric type for observable counter registration: " + getMetricEntity().getMetricType());
    }
  }

  /**
   * Reports all accumulated values to the OpenTelemetry measurement.
   * This is the callback invoked by OTel during metric collection.
   */
  private void reportToMeasurement(ObservableLongMeasurement measurement) {
    Iterable<MetricAttributesData> allData = getAllMetricAttributesData();
    if (allData == null) {
      return;
    }

    for (MetricAttributesData holder: allData) {
      if (holder.hasAdder()) {
        long value = holder.sumThenReset();
        measurement.record(value, holder.getAttributes());
      }
    }
  }

  /** Returns whether this metric entity state is for an Observable Counter */
  public final boolean isObservableCounter() {
    return isObservableCounter;
  }

  /**
   * Creates the double recording strategy for histogram types that need double precision.
   */
  private ObjDoubleConsumer<MetricAttributesData> createOtelDoubleRecordingStrategy(MetricType metricType) {
    switch (metricType) {
      case HISTOGRAM:
      case MIN_MAX_COUNT_SUM_AGGREGATIONS:
        return (holder, value) -> ((DoubleHistogram) otelMetric).record(value, holder.getAttributes());
      default:
        // For non-histogram types, delegate to long strategy
        return (holder, value) -> otelLongRecordingStrategy.accept(holder, (long) value);
    }
  }

  /**
   * Creates the long recording strategy for counter/gauge types - avoids unnecessary double conversion.
   */
  private ObjLongConsumer<MetricAttributesData> createOtelLongRecordingStrategy(MetricType metricType) {
    switch (metricType) {
      case ASYNC_COUNTER_FOR_HIGH_PERF_CASES:
      case ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES:
        return (holder, value) -> holder.add(value);
      case COUNTER:
        return (holder, value) -> ((LongCounter) otelMetric).add(value, holder.getAttributes());
      case UP_DOWN_COUNTER:
        return (holder, value) -> ((LongUpDownCounter) otelMetric).add(value, holder.getAttributes());
      case GAUGE:
        return (holder, value) -> ((LongGauge) otelMetric).set(value, holder.getAttributes());
      case HISTOGRAM:
      case MIN_MAX_COUNT_SUM_AGGREGATIONS:
        // Histograms use double, so convert here (rarely called via long path)
        return (holder, value) -> ((DoubleHistogram) otelMetric).record((double) value, holder.getAttributes());
      default:
        throw new IllegalArgumentException("Unsupported metric type: " + metricType);
    }
  }

  /**
   * Record otel metrics with MetricAttributesData (double version for histograms)
   */
  public void recordOtelMetric(double value, MetricAttributesData holder) {
    if (otelMetric != null) {
      otelDoubleRecordingStrategy.accept(holder, value);
    }
  }

  /**
   * Record otel metrics with MetricAttributesData (long version)
   */
  public void recordOtelMetric(long value, MetricAttributesData holder) {
    if (otelMetric != null) {
      otelLongRecordingStrategy.accept(holder, value);
    }
  }

  void recordTehutiMetric(double value) {
    if (tehutiSensor != null) {
      tehutiSensor.record(value);
    }
  }

  final void record(double value, Attributes attributes) {
    record(value, new MetricAttributesData(attributes));
  }

  /**
   * Records a double value using MetricAttributesData.
   * Use this for histogram metrics that need double precision.
   */
  protected final void record(double value, MetricAttributesData holder) {
    recordOtelMetric(value, holder);
    recordTehutiMetric(value);
  }

  /**
   * Records a long value using MetricAttributesData.
   * More efficient for counter/gauge metrics - avoids long->double->long conversion.
   */
  protected final void record(long value, MetricAttributesData holder) {
    recordOtelMetric(value, holder);
    recordTehutiMetric(value);
  }
}
