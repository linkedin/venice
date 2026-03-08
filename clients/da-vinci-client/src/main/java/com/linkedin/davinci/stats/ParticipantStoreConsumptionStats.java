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

  /**
   * Per-store metric state maps, keyed by store name. Each map grows lazily via {@code computeIfAbsent} and is bounded
   * by the number of stores the server is actively ingesting. Entries are not evicted — the map is bounded because only
   * stores returned by {@code getIngestingTopicsWithVersionStatusNotOnline()} generate recordings, and that set is
   * finite per server.
   *
   * <p>All per-store entries within each map share a single cluster-scoped Tehuti sensor (registered once via
   * {@code registerSensorIfAbsent}). The per-store map structure exists for OTel per-store dimension attribution only.
   *
   * <p>{@code killedPushJobsPerStore} and {@code failedKillPushJobPerStore} both map to the same OTel metric but
   * MUST remain separate maps because each distinct Tehuti binding needs its own {@code MetricEntityState} entry —
   * {@code killedPushJobsPerStore} has a Tehuti metric, {@code failedKillPushJobPerStore} does not. They can be
   * merged once Tehuti is removed.
   */
  private final Map<String, MetricEntityStateBase> killLatencyPerStore = new VeniceConcurrentHashMap<>();
  private final Map<String, MetricEntityStateOneEnum<VeniceResponseStatusCategory>> killedPushJobsPerStore =
      new VeniceConcurrentHashMap<>();
  private final Map<String, MetricEntityStateOneEnum<VeniceResponseStatusCategory>> failedKillPushJobPerStore =
      new VeniceConcurrentHashMap<>();
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

  /** Records a successful push job kill on the storage node. Tehuti + OTel (SUCCESS). */
  public void recordKilledPushJobs(String storeName) {
    killedPushJobsPerStore.computeIfAbsent(
        storeName,
        k -> MetricEntityStateOneEnum.create(
            ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity(),
            otelRepository,
            this::registerSensorIfAbsent,
            TehutiMetricName.KILLED_PUSH_JOBS,
            Collections.singletonList(new Count()),
            buildStoreDimensionsMap(k),
            VeniceResponseStatusCategory.class))
        .record(1, VeniceResponseStatusCategory.SUCCESS);
  }

  /** Records a push job kill attempt that failed ({@code killConsumptionTask} returned false). OTel-only (FAIL). */
  public void recordFailedKillPushJob(String storeName) {
    failedKillPushJobPerStore.computeIfAbsent(
        storeName,
        k -> MetricEntityStateOneEnum.create(
            ParticipantStoreConsumptionOtelMetricEntity.KILL_PUSH_JOB_COUNT.getMetricEntity(),
            otelRepository,
            buildStoreDimensionsMap(k),
            VeniceResponseStatusCategory.class))
        .record(1, VeniceResponseStatusCategory.FAIL);
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
