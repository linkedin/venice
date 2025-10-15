package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.RetryManagerMetricEntity.RETRY_BUDGET_REMAINING;
import static com.linkedin.venice.stats.RetryManagerMetricEntity.RETRY_RATE_LIMIT;
import static com.linkedin.venice.stats.RetryManagerMetricEntity.RETRY_REJECTED_COUNT;

import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.throttle.TokenBucket;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.Map;


public class RetryManagerStats extends AbstractVeniceStats {
  private final String storeName;
  private final AsyncMetricEntityStateBase retryLimitPerSecondOtel;
  private final AsyncMetricEntityStateBase retriesRemainingOtel;
  private final MetricEntityStateBase rejectedRetrySensorOtel;

  // OTel support
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final Attributes baseAttributes;

  public RetryManagerStats(
      MetricsRepository metricsRepository,
      String name,
      String storeName,
      RequestType requestType,
      RetryManager retryManager) {
    super(metricsRepository, name);
    this.storeName = storeName;

    // We use requestType == null indicates Router usage; non-null indicates FastClient usage.
    if (requestType == null) {
      this.otelRepository = null;
      this.baseDimensionsMap = null;
      this.baseAttributes = null;
    } else {
      OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
          OpenTelemetryMetricsSetup.builder(metricsRepository)
              // set all base dimensions for this stats class and build
              .setStoreName(this.storeName)
              .setRequestType(requestType)
              .setRequestRetryType(RequestRetryType.LONG_TAIL_RETRY)
              .build();
      this.otelRepository = otelData.getOtelRepository();
      this.baseDimensionsMap = otelData.getBaseDimensionsMap();
      this.baseAttributes = otelData.getBaseAttributes();
    }

    this.retryLimitPerSecondOtel = AsyncMetricEntityStateBase.create(
        RETRY_RATE_LIMIT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        RetryManagerTehutiMetricName.RETRY_LIMIT_PER_SECONDS,
        Collections.singletonList(new AsyncGauge((ignored, ignored2) -> {
          TokenBucket bucket = retryManager.getRetryTokenBucket();
          return bucket == null ? -1 : bucket.getAmortizedRefillPerSecond();
        }, RetryManagerTehutiMetricName.RETRY_LIMIT_PER_SECONDS.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        () -> {
          TokenBucket bucket = retryManager.getRetryTokenBucket();
          return bucket == null ? -1 : (long) bucket.getAmortizedRefillPerSecond();
        });

    this.retriesRemainingOtel = AsyncMetricEntityStateBase.create(
        RETRY_BUDGET_REMAINING.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        RetryManagerTehutiMetricName.RETRIES_REMAINING,
        Collections.singletonList(new AsyncGauge((ignored, ignored2) -> {
          TokenBucket bucket = retryManager.getRetryTokenBucket();
          return bucket == null ? -1 : bucket.getStaleTokenCount();
        }, RetryManagerTehutiMetricName.RETRIES_REMAINING.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        () -> {
          TokenBucket bucket = retryManager.getRetryTokenBucket();
          return bucket == null ? -1 : bucket.getStaleTokenCount();
        });

    this.rejectedRetrySensorOtel = MetricEntityStateBase.create(
        RETRY_REJECTED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        RetryManagerTehutiMetricName.REJECTED_RETRY,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);
  }

  public void recordRejectedRetry(int count) {
    rejectedRetrySensorOtel.record(count);
  }

  public enum RetryManagerTehutiMetricName implements TehutiMetricNameEnum {
    RETRY_LIMIT_PER_SECONDS, RETRIES_REMAINING, REJECTED_RETRY;

    private final String metricName;

    RetryManagerTehutiMetricName() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}
