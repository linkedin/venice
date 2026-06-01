package com.linkedin.venice.stats.routing;

import static com.linkedin.venice.stats.routing.RoutingMetricEntity.HELIX_GROUP_CALL_COUNT;
import static com.linkedin.venice.stats.routing.RoutingMetricEntity.HELIX_GROUP_CALL_TIME;
import static com.linkedin.venice.stats.routing.RoutingMetricEntity.HELIX_GROUP_COUNT;
import static com.linkedin.venice.stats.routing.RoutingMetricEntity.HELIX_GROUP_REQUEST_PENDING_REQUESTS;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.concurrent.SlidingWindowAverage;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.Metric;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class HelixGroupStats extends AbstractVeniceStats {
  /**
   * Sliding-window size for the independent per-group response-waiting-time average.
   * 30 s provides a stable latency signal for routing decisions: short enough to react to
   * degraded groups within a few seconds, long enough to smooth out individual request spikes.
   */
  static final long GROUP_RESPONSE_WAITING_TIME_WINDOW_MS = TimeUnit.SECONDS.toMillis(30);

  /**
   * Per-Helix-group OTel metric entity states and per-group response-waiting-time averages,
   * keyed by group ID. Each map grows lazily via {@code computeIfAbsent} and is bounded by the
   * number of Helix groups configured for the store (typically 3–5). Entries are not evicted —
   * the maps persist for the lifetime of this stats instance.
   *
   * <p>Exactly one of {@link #groupResponseWaitingTimeTehutiAvgMap} (legacy Tehuti
   * {@link io.tehuti.Metric} references) and {@link #groupResponseWaitingTimeIndependentAvgMap}
   * (independent {@link SlidingWindowAverage}) is non-null, selected by {@link #useSelfContainedStats}
   * at construction time. The remaining maps hold OTel {@link MetricEntityStateBase} instances and
   * Tehuti-joined recording state and are always populated.
   */
  private final VeniceConcurrentHashMap<Integer, Metric> groupResponseWaitingTimeTehutiAvgMap;
  private final VeniceConcurrentHashMap<Integer, SlidingWindowAverage> groupResponseWaitingTimeIndependentAvgMap;
  private final VeniceConcurrentHashMap<Integer, MetricEntityStateBase> groupRequestCountMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, MetricEntityStateBase> groupPendingRequestMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, MetricEntityStateBase> groupResponseWaitingTimeMap =
      new VeniceConcurrentHashMap<>();
  private final String storeName;
  private final boolean useSelfContainedStats;

  // OTel metrics
  private final MetricEntityStateBase helixGroupCount;

  // OTel support
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final Attributes baseAttributes;

  public HelixGroupStats(MetricsRepository metricsRepository) {
    this(metricsRepository, "", false);
  }

  public HelixGroupStats(MetricsRepository metricsRepository, boolean useSelfContainedStats) {
    this(metricsRepository, "", useSelfContainedStats);
  }

  public HelixGroupStats(MetricsRepository metricsRepository, String prefix) {
    this(metricsRepository, prefix, false);
  }

  /**
   * @param useSelfContainedStats {@code false} (default) reads the per-group response-waiting-time
   *                               average from the Tehuti {@link io.tehuti.metrics.stats.Avg}
   *                               metric; {@code true} reads from an independent
   *                               {@link SlidingWindowAverage} owned by this class so the routing
   *                               decision remains correct even when the Tehuti dependency is removed.
   */
  public HelixGroupStats(MetricsRepository metricsRepository, String prefix, boolean useSelfContainedStats) {
    super(metricsRepository, prefix.isEmpty() ? "HelixGroupStats" : prefix + "_HelixGroupStats");
    this.storeName = prefix;
    this.useSelfContainedStats = useSelfContainedStats;
    this.groupResponseWaitingTimeTehutiAvgMap = useSelfContainedStats ? null : new VeniceConcurrentHashMap<>();
    this.groupResponseWaitingTimeIndependentAvgMap = useSelfContainedStats ? new VeniceConcurrentHashMap<>() : null;
    // When storeName is empty, it means the stats is used for Venice Router.
    if (storeName.isEmpty()) {
      this.otelRepository = null;
      this.baseDimensionsMap = null;
      this.baseAttributes = null;
    } else {
      // When storeName is not empty, it means the stats is used for Venice Client and prefix is the store name.
      OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
          OpenTelemetryMetricsSetup.builder(metricsRepository)
              // set all base dimensions for this stats class and build
              .setStoreName(storeName)
              .build();

      this.otelRepository = otelData.getOtelRepository();
      this.baseDimensionsMap = otelData.getBaseDimensionsMap();
      this.baseAttributes = otelData.getBaseAttributes();
    }

    // Initialize OTel metrics for helix group count
    this.helixGroupCount = MetricEntityStateBase.create(
        HELIX_GROUP_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        HelixGroupTehutiMetricName.GROUP_COUNT,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        baseAttributes);
  }

  public void recordGroupNum(int groupNum) {
    helixGroupCount.record(groupNum);
  }

  private MetricEntityStateBase buildHelixGroupCallCount(int groupId) {
    OtelSetupData otelSetup = buildOtelSetupForGroup(groupId);
    return MetricEntityStateBase.create(
        HELIX_GROUP_CALL_COUNT.getMetricEntity(),
        otelSetup.otelRepository,
        this::registerSensor,
        HelixGroupTehutiMetricDynamicName.of(HelixGroupTehutiMetricName.GROUP_REQUEST, groupId),
        Collections.singletonList(new OccurrenceRate()),
        otelSetup.baseDimensionsMap,
        otelSetup.baseAttributes);
  }

  public void recordGroupRequest(int groupId) {
    groupRequestCountMap.computeIfAbsent(groupId, id -> buildHelixGroupCallCount(groupId)).record(1);
  }

  private MetricEntityStateBase buildHelixGroupPendingRequest(int groupId) {
    OtelSetupData otelSetup = buildOtelSetupForGroup(groupId);
    return MetricEntityStateBase.create(
        HELIX_GROUP_REQUEST_PENDING_REQUESTS.getMetricEntity(),
        otelSetup.otelRepository,
        this::registerSensor,
        HelixGroupTehutiMetricDynamicName.of(HelixGroupTehutiMetricName.GROUP_PENDING_REQUEST, groupId),
        Collections.singletonList(new Avg()),
        otelSetup.baseDimensionsMap,
        otelSetup.baseAttributes);
  }

  public void recordGroupPendingRequest(int groupId, int value) {
    MetricEntityStateBase pendingRequest =
        groupPendingRequestMap.computeIfAbsent(groupId, id -> buildHelixGroupPendingRequest(groupId));
    pendingRequest.record(value);
  }

  private MetricEntityStateBase buildHelixGroupResponseWaitingTime(int groupId, MeasurableStat avgStat) {
    OtelSetupData otelSetup = buildOtelSetupForGroup(groupId);
    return MetricEntityStateBase.create(
        HELIX_GROUP_CALL_TIME.getMetricEntity(),
        otelSetup.otelRepository,
        this::registerSensor,
        HelixGroupTehutiMetricDynamicName.of(HelixGroupTehutiMetricName.GROUP_RESPONSE_WAITING_TIME, groupId),
        Collections.singletonList(avgStat),
        otelSetup.baseDimensionsMap,
        otelSetup.baseAttributes);
  }

  public void recordGroupResponseWaitingTime(int groupId, double responseWaitingTime) {
    // Tehuti+OTel joint recording — always active, regardless of which read path is selected.
    MetricEntityStateBase groupResponseWaitingTime = groupResponseWaitingTimeMap.computeIfAbsent(groupId, id -> {
      MeasurableStat avgStat = new Avg();
      MetricEntityStateBase waitTime = buildHelixGroupResponseWaitingTime(groupId, avgStat);
      if (!useSelfContainedStats) {
        // Legacy path: cache the Tehuti Metric reference for routing logic to read.
        groupResponseWaitingTimeTehutiAvgMap
            .put(groupId, getMetricsRepository().getMetric(getMetricFullName(waitTime.getTehutiSensor(), avgStat)));
      }
      return waitTime;
    });
    groupResponseWaitingTime.record(responseWaitingTime);

    if (useSelfContainedStats) {
      // Independent sliding-window average owned by this class — routing decision stays correct
      // even when Tehuti is disabled.
      groupResponseWaitingTimeIndependentAvgMap
          .computeIfAbsent(groupId, id -> new SlidingWindowAverage(GROUP_RESPONSE_WAITING_TIME_WINDOW_MS))
          .record(responseWaitingTime);
    }
  }

  public double getGroupResponseWaitingTimeAvg(int groupId) {
    double avgLatency;
    if (useSelfContainedStats) {
      SlidingWindowAverage counter = groupResponseWaitingTimeIndependentAvgMap.get(groupId);
      if (counter == null) {
        return -1;
      }
      avgLatency = counter.average();
    } else {
      Metric metric = groupResponseWaitingTimeTehutiAvgMap.get(groupId);
      if (metric == null) {
        return -1;
      }
      avgLatency = metric.value();
    }
    if (Double.isNaN(avgLatency)) {
      return -1;
    }
    return avgLatency;
  }

  /**
   * Helper class to hold OpenTelemetry setup data for a specific helix group.
   */
  private static class OtelSetupData {
    final VeniceOpenTelemetryMetricsRepository otelRepository;
    final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
    final Attributes baseAttributes;

    OtelSetupData(
        VeniceOpenTelemetryMetricsRepository otelRepository,
        Map<VeniceMetricsDimensions, String> baseDimensionsMap,
        Attributes baseAttributes) {
      this.otelRepository = otelRepository;
      this.baseDimensionsMap = baseDimensionsMap;
      this.baseAttributes = baseAttributes;
    }
  }

  /**
   * Builds OpenTelemetry setup data for a specific helix group.
   * Returns null values if storeName is empty (Router mode).
   */
  private OtelSetupData buildOtelSetupForGroup(int groupId) {
    if (storeName.isEmpty()) {
      return new OtelSetupData(null, null, null);
    }

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(getMetricsRepository())
            .setStoreName(storeName)
            .setHelixGroupId(groupId)
            .build();

    return new OtelSetupData(
        otelData.getOtelRepository(),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
  }

  /**
   * Metric names for tehuti metrics used in this class.
   */
  public enum HelixGroupTehutiMetricName implements TehutiMetricNameEnum {
    GROUP_COUNT, GROUP_REQUEST, GROUP_PENDING_REQUEST, GROUP_RESPONSE_WAITING_TIME
  }

  /**
   * Dynamic metric names for tehuti metrics used in this class.
   * The metric name is generated based on the group id.
   * e.g., for group id 0, the metric name for GROUP_REQUEST will be "group_0_request"
   */
  public static class HelixGroupTehutiMetricDynamicName implements TehutiMetricNameEnum {
    private final String metricName;

    private HelixGroupTehutiMetricDynamicName(HelixGroupTehutiMetricName type, int groupId) {
      // Extract the suffix after "group_" and insert groupId
      // e.g., "group_request" -> "group_0_request"
      String baseMetricName = type.getMetricName();
      String suffix = baseMetricName.substring("group_".length());
      this.metricName = "group_" + groupId + "_" + suffix;
    }

    public static HelixGroupTehutiMetricDynamicName of(HelixGroupTehutiMetricName type, int groupId) {
      return new HelixGroupTehutiMetricDynamicName(type, groupId);
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}
