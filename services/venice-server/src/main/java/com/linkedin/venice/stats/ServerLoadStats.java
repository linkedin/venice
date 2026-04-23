package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.ServerLoadOtelMetricEntity.REJECTION_RATIO;
import static com.linkedin.davinci.stats.ServerLoadOtelMetricEntity.REQUEST_COUNT;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceServerLoadRequestOutcome;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;


public class ServerLoadStats extends AbstractVeniceStats {
  /** Tehuti metric names for sensors managed via joint API or plain Sensor. */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    REJECTED_REQUEST, ACCEPTED_REQUEST, REJECTION_RATIO
  }

  // Tehuti-only: total request count (OTel derives total from sum of ACCEPTED + REJECTED)
  private final Sensor totalRequestSensor;

  // Joint Tehuti+OTel: rejected requests (Tehuti OccurrenceRate + OTel COUNTER with REJECTED dimension)
  private final MetricEntityStateOneEnum<VeniceServerLoadRequestOutcome> rejectedRequestMetric;

  // Joint Tehuti+OTel: accepted requests (Tehuti OccurrenceRate + OTel COUNTER with ACCEPTED dimension)
  private final MetricEntityStateOneEnum<VeniceServerLoadRequestOutcome> acceptedRequestMetric;

  // Joint Tehuti+OTel: rejection ratio (Tehuti Avg/Max + OTel MIN_MAX_COUNT_SUM_AGGREGATIONS)
  private final MetricEntityStateBase rejectionRatioMetric;

  public ServerLoadStats(MetricsRepository metricsRepository, String name, String clusterName) {
    super(metricsRepository, name);

    // Tehuti-only: OTel total is derived at query time from sum(ACCEPTED + REJECTED)
    totalRequestSensor = registerSensorIfAbsent("total_request", new OccurrenceRate());

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    // Two MetricEntityStateOneEnum instances sharing the same OTel metric (REQUEST_COUNT),
    // each bound to its own Tehuti sensor. OTel deduplicates by metric name internally.
    rejectedRequestMetric = MetricEntityStateOneEnum.create(
        REQUEST_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.REJECTED_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        VeniceServerLoadRequestOutcome.class);

    acceptedRequestMetric = MetricEntityStateOneEnum.create(
        REQUEST_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.ACCEPTED_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        VeniceServerLoadRequestOutcome.class);

    rejectionRatioMetric = MetricEntityStateBase.create(
        REJECTION_RATIO.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.REJECTION_RATIO,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);
  }

  /** Tehuti-only: total request count. OTel derives total from sum(ACCEPTED + REJECTED). */
  public void recordTotalRequest() {
    totalRequestSensor.record();
  }

  public void recordRejectedRequest() {
    rejectedRequestMetric.record(1, VeniceServerLoadRequestOutcome.REJECTED);
  }

  public void recordAcceptedRequest() {
    acceptedRequestMetric.record(1, VeniceServerLoadRequestOutcome.ACCEPTED);
  }

  public void recordRejectionRatio(double rejectionRatio) {
    rejectionRatioMetric.record(rejectionRatio);
  }
}
