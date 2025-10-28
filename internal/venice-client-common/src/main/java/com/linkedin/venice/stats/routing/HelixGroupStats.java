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
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.Map;


public class HelixGroupStats extends AbstractVeniceStats {
  private final VeniceConcurrentHashMap<Integer, Metric> groupResponseWaitingTimeAvgMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, MetricEntityStateBase> groupRequestCountMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, MetricEntityStateBase> groupPendingRequestMap =
      new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<Integer, MetricEntityStateBase> groupResponseWaitingTimeMap =
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
    GROUP_COUNT, GROUP_REQUEST, GROUP_PENDING_REQUEST, GROUP_RESPONSE_WAITING_TIME;

    private final String metricName;

    HelixGroupTehutiMetricName() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
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
