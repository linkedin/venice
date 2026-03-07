package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ParticipantStoreConsumptionStats extends AbstractVeniceStats {
  private static final String NAME_SUFFIX = "-participant_store_consumption_task";

  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /** Heartbeat metric for the participant store consumption task. */
  private final MetricEntityStateBase heartbeatMetric;

  /** Counter for the number of times that the participant store consumption task failed to start. */
  private final MetricEntityStateBase failedInitializationMetric;

  /** Per-store kill latency metric states, keyed by store name. */
  private final Map<String, MetricEntityStateBase> killLatencyPerStore = new VeniceConcurrentHashMap<>();

  /** Per-store kill attempt count metric states (SUCCESS/FAIL dimension), keyed by store name. */
  private final Map<String, MetricEntityStateOneEnum<VeniceResponseStatusCategory>> killCountPerStore =
      new VeniceConcurrentHashMap<>();

  /** Per-store kill failed consumption count metric states, keyed by store name. */
  private final Map<String, MetricEntityStateBase> killFailedConsumptionPerStore = new VeniceConcurrentHashMap<>();

  enum TehutiMetricName implements TehutiMetricNameEnum {
    KILL_PUSH_JOB_LATENCY, KILLED_PUSH_JOBS, FAILED_INITIALIZATION, KILL_PUSH_JOB_FAILED_CONSUMPTION, HEARTBEAT
  }

  /**
   * @param clusterName the cluster that this server belongs to, used as the {@code VENICE_CLUSTER_NAME} base dimension
   *                    on all OTel metrics emitted by this class.
   */
  public ParticipantStoreConsumptionStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, clusterName + NAME_SUFFIX);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    this.heartbeatMetric = MetricEntityStateBase.create(
        ParticipantStoreConsumptionOtelMetricEntity.HEARTBEAT_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.HEARTBEAT,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    this.failedInitializationMetric = MetricEntityStateBase.create(
        ParticipantStoreConsumptionOtelMetricEntity.FAILED_INITIALIZATION_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TehutiMetricName.FAILED_INITIALIZATION,
        Collections.singletonList(new Count()),
        baseDimensionsMap,
        baseAttributes);
  }

  private Map<VeniceMetricsDimensions, String> buildStoreDimensionsMap(String storeName) {
    Map<VeniceMetricsDimensions, String> map = new HashMap<>(baseDimensionsMap);
    map.put(VENICE_STORE_NAME, storeName);
    return Collections.unmodifiableMap(map);
  }

  private Attributes buildStoreAttributes(
      MetricEntity metricEntity,
      Map<VeniceMetricsDimensions, String> storeDimensionsMap) {
    return otelRepository != null ? otelRepository.createAttributes(metricEntity, storeDimensionsMap) : null;
  }

  private MetricEntityStateBase createPerStoreBaseMetric(
      ParticipantStoreConsumptionOtelMetricEntity otelMetric,
      TehutiMetricName tehutiName,
      List<MeasurableStat> tehutiStats,
      String storeName) {
    MetricEntity metricEntity = otelMetric.getMetricEntity();
    Map<VeniceMetricsDimensions, String> storeDimensionsMap = buildStoreDimensionsMap(storeName);
    return MetricEntityStateBase.create(
        metricEntity,
        otelRepository,
        this::registerSensorIfAbsent,
        tehutiName,
        tehutiStats,
        storeDimensionsMap,
        buildStoreAttributes(metricEntity, storeDimensionsMap));
  }

  /**
   * Records the latency in ms from when the kill signal was generated in the child controller to when the kill is
   * performed on the storage node.
   */
  public void recordKillPushJobLatency(String storeName, double latencyInMs) {
    killLatencyPerStore
        .computeIfAbsent(
            storeName,
            k -> createPerStoreBaseMetric(
                ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_LATENCY,
                TehutiMetricName.KILL_PUSH_JOB_LATENCY,
                Arrays.asList(new Avg(), new Max()),
                k))
        .record(latencyInMs);
  }

  /** Records a confirmed push job kill on the storage node. */
  public void recordKilledPushJobs(String storeName) {
    getOrCreateKillCountState(storeName).record(1, VeniceResponseStatusCategory.SUCCESS);
  }

  /** Records a push job kill attempt that failed ({@code killConsumptionTask} returned false). */
  public void recordFailedKillPushJob(String storeName) {
    getOrCreateKillCountState(storeName).record(1, VeniceResponseStatusCategory.FAIL);
  }

  private MetricEntityStateOneEnum<VeniceResponseStatusCategory> getOrCreateKillCountState(String storeName) {
    return killCountPerStore.computeIfAbsent(
        storeName,
        k -> MetricEntityStateOneEnum.create(
            ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity(),
            otelRepository,
            this::registerSensorIfAbsent,
            TehutiMetricName.KILLED_PUSH_JOBS,
            Collections.singletonList(new Count()),
            buildStoreDimensionsMap(k),
            VeniceResponseStatusCategory.class));
  }

  public void recordFailedInitialization() {
    failedInitializationMetric.record(1);
  }

  /**
   * Records an exception thrown during consumption of the participant store, specifically for
   * {@link com.linkedin.venice.participant.protocol.KillPushJob} records.
   */
  public void recordKillPushJobFailedConsumption(String storeName) {
    killFailedConsumptionPerStore
        .computeIfAbsent(
            storeName,
            k -> createPerStoreBaseMetric(
                ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_FAILED_CONSUMPTION_COUNT,
                TehutiMetricName.KILL_PUSH_JOB_FAILED_CONSUMPTION,
                Collections.singletonList(new Count()),
                k))
        .record(1);
  }

  public void recordHeartbeat() {
    heartbeatMetric.record(1);
  }
}
