package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.RetryManagerMetricEntity.RETRY_RATE_LIMIT_REJECTION_COUNT;
import static com.linkedin.venice.stats.RetryManagerMetricEntity.RETRY_RATE_LIMIT_REMAINING_TOKENS;
import static com.linkedin.venice.stats.RetryManagerMetricEntity.RETRY_RATE_LIMIT_TARGET_TOKENS;

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
  private final AsyncMetricEntityStateBase retryLimitPerSecond;
  private final AsyncMetricEntityStateBase retriesRemaining;
  private final MetricEntityStateBase rejectedRetry;

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

    this.retryLimitPerSecond = AsyncMetricEntityStateBase.create(
        RETRY_RATE_LIMIT_TARGET_TOKENS.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
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

    this.retriesRemaining = AsyncMetricEntityStateBase.create(
        RETRY_RATE_LIMIT_REMAINING_TOKENS.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
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

    this.rejectedRetry = MetricEntityStateBase.create(
        RETRY_RATE_LIMIT_REJECTION_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        RetryManagerTehutiMetricName.REJECTED_RETRY,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);
  }

  public void recordRejectedRetry(int count) {
    rejectedRetry.record(count);
  }

  public enum RetryManagerTehutiMetricName implements TehutiMetricNameEnum {
    RETRY_LIMIT_PER_SECONDS, RETRIES_REMAINING, REJECTED_RETRY
  }
}
