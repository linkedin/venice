package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.LiveStateResolver;
import com.linkedin.venice.stats.metrics.AsyncMetricResolvers.ValueResolver;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;


/**
 * This version of {@link AsyncMetricEntityState} is used when the metric entity has no dynamic dimensions.
 * The base {@link Attributes} that are common for all invocation of this instance are passed in the
 * constructor and used during async callback recording.
 */
public class AsyncMetricEntityStateBase extends AsyncMetricEntityState {
  private <S> AsyncMetricEntityStateBase(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      LiveStateResolver<S> liveStateResolver,
      ValueResolver<S> valueResolver) {
    super(
        metricEntity,
        otelRepository,
        baseDimensionsMap,
        registerTehutiSensorFn,
        tehutiMetricNameEnum,
        tehutiMetricStats,
        liveStateResolver,
        valueResolver,
        baseAttributes);
    validateBaseAttributes(metricEntity, baseAttributes, baseDimensionsMap);
  }

  private void validateBaseAttributes(
      MetricEntity metricEntity,
      Attributes baseAttributes,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap) {
    validateRequiredDimensions(metricEntity, baseAttributes, baseDimensionsMap);
    if (emitOpenTelemetryMetrics()) {
      Validate.notNull(
          baseAttributes,
          "Base attributes cannot be null for MetricEntityStateBase for metric: " + metricEntity.getMetricName());
    }
  }

  /**
   * Creates an async gauge that {@code scope} closes. {@code valueResolver} runs only for non-null state from
   * {@code liveStateResolver}; see {@link GaugeObservation}.
   */
  public static <S> AsyncMetricEntityStateBase createWithState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      MetricScope scope,
      @Nonnull LiveStateResolver<S> liveStateResolver,
      @Nonnull ValueResolver<S> valueResolver) {
    return Objects.requireNonNull(scope, "scope")
        .register(
            new AsyncMetricEntityStateBase(
                metricEntity,
                otelRepository,
                null,
                null,
                Collections.emptyList(),
                baseDimensionsMap,
                baseAttributes,
                liveStateResolver,
                valueResolver));
  }

  /** Same as above, plus a Tehuti sensor; the resolvers apply to OTel only. */
  public static <S> AsyncMetricEntityStateBase createWithState(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      TehutiSensorRegistrationFunction registerTehutiSensorFn,
      TehutiMetricNameEnum tehutiMetricNameEnum,
      List<MeasurableStat> tehutiMetricStats,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      MetricScope scope,
      @Nonnull LiveStateResolver<S> liveStateResolver,
      @Nonnull ValueResolver<S> valueResolver) {
    return Objects.requireNonNull(scope, "scope")
        .register(
            new AsyncMetricEntityStateBase(
                metricEntity,
                otelRepository,
                registerTehutiSensorFn,
                tehutiMetricNameEnum,
                tehutiMetricStats,
                baseDimensionsMap,
                baseAttributes,
                liveStateResolver,
                valueResolver));
  }
}
