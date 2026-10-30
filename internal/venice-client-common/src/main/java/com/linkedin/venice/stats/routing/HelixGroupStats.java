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
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.Metric;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.Map;


public class HelixGroupStats extends AbstractVeniceStats {
  /**
   * Per-Helix-group OTel metric entity states and Tehuti metric references, keyed by group ID. Each map grows
   * lazily via {@code computeIfAbsent} and is bounded by the number of Helix groups configured for the store
   * (typically 3–5). Entries are not evicted — the maps persist for the lifetime of this stats instance.
   * {@code groupResponseWaitingTimeAvgMap} holds Tehuti {@link io.tehuti.metrics.Metric} references; the
   * remaining maps hold OTel {@link MetricEntityStateBase} instances.
   */
  private final VeniceConcurrentHashMap<Integer, Metric> groupResponseWaitingTimeAvgMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, MetricEntityStateBase> groupRequestCountMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, MetricEntityStateBase> groupPendingRequestMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, MetricEntityStateBase> groupResponseWaitingTimeMap =
      new VeniceConcurrentHashMap<>();
  /**
   * Dedicated per-(group, key-count bucket) routing-latency signal that the latency-adaptive group selector reads
   * back when it is configured. Kept entirely separate from the shared {@code groupResponseWaitingTime*} metric
   * above so that bucketizing the routing signal perturbs neither least-loaded routing nor the existing per-group
   * dashboards. Tehuti-only (exported to InGraphs/RRD like the other per-group metrics, with no OTel dimension),
   * keyed first by group id, then by the bucket's inclusive lower key-count bound as produced by
   * {@link LatencyAdaptiveGroupSelector#bucketKeyForKeyCount(int)}. Each inner map grows lazily and is bounded by the
   * number of configured routing buckets (a handful); entries are never evicted. The sensor map holds the Tehuti
   * {@link Sensor} used to record, while the avg/count maps hold the {@link Metric} handles the selector reads.
   */
  private final VeniceConcurrentHashMap<Integer, VeniceConcurrentHashMap<Integer, Sensor>> routingLatencySensorMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, VeniceConcurrentHashMap<Integer, Metric>> routingLatencyAvgMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, VeniceConcurrentHashMap<Integer, Metric>> routingLatencyCountMap =
      new VeniceConcurrentHashMap<>();
  private final String storeName;

  // OTel metrics
  private final MetricEntityStateBase helixGroupCount;

  // OTel support
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final Attributes baseAttributes;

  public HelixGroupStats(MetricsRepository metricsRepository) {
    this(metricsRepository, "");
  }

  public HelixGroupStats(MetricsRepository metricsRepository, String prefix) {
    super(metricsRepository, prefix.isEmpty() ? "HelixGroupStats" : prefix + "_HelixGroupStats");
    this.storeName = prefix;
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
        (sensorName, stats) -> registerSensor(sensorName, stats),
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
        (sensorName, stats) -> registerSensor(sensorName, stats),
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
        (sensorName, stats) -> registerSensor(sensorName, stats),
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
        (sensorName, stats) -> registerSensor(sensorName, stats),
        HelixGroupTehutiMetricDynamicName.of(HelixGroupTehutiMetricName.GROUP_RESPONSE_WAITING_TIME, groupId),
        Collections.singletonList(avgStat),
        otelSetup.baseDimensionsMap,
        otelSetup.baseAttributes);
  }

  public void recordGroupResponseWaitingTime(int groupId, double responseWaitingTime) {
    MetricEntityStateBase groupResponseWaitingTime = groupResponseWaitingTimeMap.computeIfAbsent(groupId, id -> {
      MeasurableStat avgStat = new Avg();
      MetricEntityStateBase waitTime = buildHelixGroupResponseWaitingTime(groupId, avgStat);
      groupResponseWaitingTimeAvgMap
          .put(groupId, getMetricsRepository().getMetric(getMetricFullName(waitTime.getTehutiSensor(), avgStat)));
      return waitTime;
    });
    groupResponseWaitingTime.record(responseWaitingTime);
  }

  public double getGroupResponseWaitingTimeAvg(int groupId) {
    Metric groupResponseWaitingTimeAvgMetric = groupResponseWaitingTimeAvgMap.get(groupId);
    if (groupResponseWaitingTimeAvgMetric == null) {
      return -1;
    }
    double avgLatency = groupResponseWaitingTimeAvgMetric.value();
    if (Double.isNaN(avgLatency)) {
      return -1;
    }
    return avgLatency;
  }

  /**
   * Lazily registers the Tehuti sensor (Avg for the routing signal, Count for the sample floor) for one
   * {@code (group, bucket)} pair and caches the two {@link Metric} handles the selector reads back. The sensor name
   * encodes both the group and the bucket's inclusive lower key-count bound, e.g. {@code group_0_bucket_6_...}.
   */
  private Sensor buildRoutingLatencySensor(int groupId, int keyCountBucket) {
    MeasurableStat avgStat = new Avg();
    MeasurableStat countStat = new Count();
    Sensor sensor = registerSensor(
        "group_" + groupId + "_bucket_" + keyCountBucket + "_routing_response_waiting_time",
        avgStat,
        countStat);
    routingLatencyAvgMap.computeIfAbsent(groupId, id -> new VeniceConcurrentHashMap<>())
        .put(keyCountBucket, getMetricsRepository().getMetric(getMetricFullName(sensor, avgStat)));
    routingLatencyCountMap.computeIfAbsent(groupId, id -> new VeniceConcurrentHashMap<>())
        .put(keyCountBucket, getMetricsRepository().getMetric(getMetricFullName(sensor, countStat)));
    return sensor;
  }

  /**
   * Records a routing-latency sample for {@code groupId} into the key-count {@code bucket} it belongs to (the bucket's
   * inclusive lower bound, from {@link LatencyAdaptiveGroupSelector#bucketKeyForKeyCount(int)}). Feeds only the
   * latency-adaptive selector; the shared {@link #recordGroupResponseWaitingTime(int, double)} signal is left
   * untouched for least-loaded routing and existing dashboards.
   */
  public void recordRoutingLatency(int groupId, int keyCountBucket, double responseWaitingTime) {
    routingLatencySensorMap.computeIfAbsent(groupId, id -> new VeniceConcurrentHashMap<>())
        .computeIfAbsent(keyCountBucket, bucket -> buildRoutingLatencySensor(groupId, bucket))
        .record(responseWaitingTime);
  }

  /**
   * Average measured routing latency (ms) for {@code groupId} in the given key-count {@code bucket}, or {@code -1} if
   * the bucket has no sample yet. Matches {@link LatencyAdaptiveGroupSelector.GroupBucketLatencyProvider}.
   */
  public double getRoutingLatencyAvg(int groupId, int keyCountBucket) {
    Metric avgMetric = getRoutingMetric(routingLatencyAvgMap, groupId, keyCountBucket);
    if (avgMetric == null) {
      return -1;
    }
    double avgLatency = avgMetric.value();
    if (Double.isNaN(avgLatency)) {
      return -1;
    }
    return avgLatency;
  }

  /**
   * Number of routing-latency samples recorded for {@code groupId} in the given key-count {@code bucket}, or {@code 0}
   * if none. Matches {@link LatencyAdaptiveGroupSelector.GroupBucketSampleCountProvider} and drives the selector's
   * minimum-sample floor below which a bucket is treated as not yet measured for that group.
   */
  public long getRoutingLatencySampleCount(int groupId, int keyCountBucket) {
    Metric countMetric = getRoutingMetric(routingLatencyCountMap, groupId, keyCountBucket);
    if (countMetric == null) {
      return 0L;
    }
    double count = countMetric.value();
    if (Double.isNaN(count) || count < 0) {
      return 0L;
    }
    return (long) count;
  }

  private static Metric getRoutingMetric(
      VeniceConcurrentHashMap<Integer, VeniceConcurrentHashMap<Integer, Metric>> byGroup,
      int groupId,
      int keyCountBucket) {
    VeniceConcurrentHashMap<Integer, Metric> byBucket = byGroup.get(groupId);
    return byBucket == null ? null : byBucket.get(keyCountBucket);
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
