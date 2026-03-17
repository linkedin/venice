package com.linkedin.venice.stats;

import com.linkedin.davinci.stats.ServerReadQuotaOtelMetricEntity;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.dimensions.QuotaRequestOutcome;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Rate;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;


/**
 * For measuring quota requests and rejections for a given store.
 * Specifically not using {@link com.linkedin.davinci.stats.VeniceVersionedStats} due to sophisticated needs:
 *   1. Only requestedQPS, requestedKPS, and (usageRatio) nodeKpsResponsibility need to be versioned. We'd still like to
 *      report rejections at the store level, regardless if it came from current or backup version due to stale routing.
 *   2. VeniceVersionedStats is tracking current and future. However, for the purpose of quota we only care about
 *      current and backup.
 *
 * <p>OTel metrics use high-perf async counters ({@code ASYNC_COUNTER_FOR_HIGH_PERF_CASES}) with
 * outcome + version role dimensions, recorded separately from Tehuti sensors. Tehuti sensors
 * are unversioned (no role dimension) so they are recorded directly, not via joint API.
 */
public class ServerReadQuotaUsageStats extends AbstractVeniceStats {
  /**
   * Tehuti metric name enum for all read quota usage sensors.
   * Each constant maps 1:1 to an existing Tehuti sensor name.
   */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    CURRENT_QUOTA_REQUEST, BACKUP_QUOTA_REQUEST, CURRENT_QUOTA_REQUEST_KEY_COUNT, BACKUP_QUOTA_REQUEST_KEY_COUNT,
    QUOTA_REJECTED_REQUEST, QUOTA_REJECTED_KEY_COUNT, QUOTA_UNINTENTIONALLY_ALLOWED_KEY_COUNT,
    QUOTA_REQUESTED_USAGE_RATIO
  }

  /**
   * OTel high-perf counter states, keyed by [outcome][role]. Each has its own pre-built Attributes
   * with {store, cluster, outcome, role} baked in. Uses LongAdder internally for lock-free recording.
   * <p>3 outcomes × 3 roles = 9 instances per metric, 18 total across request.count and key.count.
   */
  private final EnumMap<QuotaRequestOutcome, EnumMap<VersionRole, MetricEntityStateBase>> requestCountByOutcomeAndRole;
  private final EnumMap<QuotaRequestOutcome, EnumMap<VersionRole, MetricEntityStateBase>> keyCountByOutcomeAndRole;

  /** Tehuti-only sensors for rejected QPS/KPS (unversioned Rate) */
  private final Sensor rejectedQPSSensor;
  private final Sensor rejectedKPSSensor;
  /**
   * Tehuti-only sensor for unintentionally allowed events. Uses {@code Count} stat which
   * increments by 1 per recording (event count), not by the rcu value. The OTel counterpart
   * ({@code READ_QUOTA_KEY_COUNT} with ALLOWED_UNINTENTIONALLY) records the actual rcu — this
   * is an intentional semantic divergence preserved from the original Tehuti-only design.
   */
  private final Sensor allowedUnintentionallyKPSSensor;

  /**
   * Per-version stats for tracking requested QPS/KPS and node quota responsibility.
   * Bounded by the number of active versions (typically 2-3: current + backup + optional future).
   * Entries are evicted via {@link #removeVersion(int)}.
   */
  private final VeniceConcurrentHashMap<Integer, ServerReadQuotaVersionedStats> versionedStats =
      new VeniceConcurrentHashMap<>();
  private final AtomicInteger currentVersion = new AtomicInteger(0);
  private final AtomicInteger backupVersion = new AtomicInteger(0);
  private final boolean emitOtelMetrics;
  private final Time time;
  private final MetricConfig metricConfig;

  public ServerReadQuotaUsageStats(MetricsRepository metricsRepository, String name) {
    this(metricsRepository, name, new SystemTime(), null);
  }

  public ServerReadQuotaUsageStats(MetricsRepository metricsRepository, String name, Time time) {
    this(metricsRepository, name, time, null);
  }

  public ServerReadQuotaUsageStats(MetricsRepository metricsRepository, String name, Time time, String clusterName) {
    super(metricsRepository, name);
    this.time = time;
    this.metricConfig = new MetricConfig();

    // Set up OTel. isTotalStats() is inherited from AbstractVeniceStats (detects "total" name prefix).
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelSetup =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .setStoreName(name)
            .setClusterName(clusterName)
            .isTotalStats(isTotalStats())
            .build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelSetup.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelSetup.getBaseDimensionsMap();
    Attributes baseAttributes = otelSetup.getBaseAttributes();

    // --- Tehuti AsyncGauge sensors: requested QPS and KPS per version role ---
    // These are Tehuti-only (no OTel counterpart): OTel uses counters instead.
    registerSensor(
        TehutiMetricName.CURRENT_QUOTA_REQUEST.getMetricName(),
        new AsyncGauge(
            (ignored, ignored2) -> getVersionedRequestedQPS(currentVersion.get()),
            TehutiMetricName.CURRENT_QUOTA_REQUEST.getMetricName()));
    registerSensor(
        TehutiMetricName.BACKUP_QUOTA_REQUEST.getMetricName(),
        new AsyncGauge(
            (ignored, ignored2) -> getVersionedRequestedQPS(backupVersion.get()),
            TehutiMetricName.BACKUP_QUOTA_REQUEST.getMetricName()));
    registerSensor(
        TehutiMetricName.CURRENT_QUOTA_REQUEST_KEY_COUNT.getMetricName(),
        new AsyncGauge(
            (ignored, ignored2) -> getVersionedRequestedKPS(currentVersion.get()),
            TehutiMetricName.CURRENT_QUOTA_REQUEST_KEY_COUNT.getMetricName()));
    registerSensor(
        TehutiMetricName.BACKUP_QUOTA_REQUEST_KEY_COUNT.getMetricName(),
        new AsyncGauge(
            (ignored, ignored2) -> getVersionedRequestedKPS(backupVersion.get()),
            TehutiMetricName.BACKUP_QUOTA_REQUEST_KEY_COUNT.getMetricName()));

    // --- Tehuti synchronous sensors: rejection and unintentional allowance ---
    // These are recorded directly (not via joint API) because OTel counters have
    // per-role granularity that Tehuti sensors don't.
    rejectedQPSSensor = registerSensor(TehutiMetricName.QUOTA_REJECTED_REQUEST.getMetricName(), new Rate());
    rejectedKPSSensor = registerSensor(TehutiMetricName.QUOTA_REJECTED_KEY_COUNT.getMetricName(), new Rate());
    allowedUnintentionallyKPSSensor =
        registerSensor(TehutiMetricName.QUOTA_UNINTENTIONALLY_ALLOWED_KEY_COUNT.getMetricName(), new Count());

    // --- AsyncDoubleGauge: usage ratio (joint Tehuti + OTel, no VersionRole dimension) ---
    // OTel records the raw ratio as a double (e.g., 0.75 = 75% usage). NaN (uninitialized) maps to 0.0.
    AsyncMetricEntityStateBase.create(
        ServerReadQuotaOtelMetricEntity.READ_QUOTA_USAGE_RATIO.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        TehutiMetricName.QUOTA_REQUESTED_USAGE_RATIO,
        Collections.singletonList(
            new AsyncGauge(
                (ignored, ignored2) -> getReadQuotaUsageRatio(),
                TehutiMetricName.QUOTA_REQUESTED_USAGE_RATIO.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        (DoubleSupplier) () -> {
          Double ratio = getReadQuotaUsageRatio();
          return ratio.isNaN() ? 0.0 : ratio;
        });

    // --- OTel high-perf counters: request.count and key.count with outcome+role dimensions ---
    // Each (outcome, role) pair gets its own MetricEntityStateBase with pre-built Attributes.
    // OTel-only: Tehuti sensors are recorded separately above.
    this.emitOtelMetrics = otelSetup.emitOpenTelemetryMetrics();
    requestCountByOutcomeAndRole = buildCounterMap(
        ServerReadQuotaOtelMetricEntity.READ_QUOTA_REQUEST_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        baseAttributes,
        emitOtelMetrics);
    keyCountByOutcomeAndRole = buildCounterMap(
        ServerReadQuotaOtelMetricEntity.READ_QUOTA_KEY_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        baseAttributes,
        emitOtelMetrics);
  }

  /**
   * Builds an EnumMap of EnumMap for [QuotaRequestOutcome][VersionRole] -> MetricEntityStateBase.
   * Each instance has pre-built Attributes with {store, cluster, outcome, role} baked in.
   */
  private static EnumMap<QuotaRequestOutcome, EnumMap<VersionRole, MetricEntityStateBase>> buildCounterMap(
      MetricEntity metricEntity,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Map<VeniceMetricsDimensions, String> baseDimensionsMap,
      Attributes baseAttributes,
      boolean otelEnabled) {
    EnumMap<QuotaRequestOutcome, EnumMap<VersionRole, MetricEntityStateBase>> outerMap =
        new EnumMap<>(QuotaRequestOutcome.class);
    for (QuotaRequestOutcome outcome: QuotaRequestOutcome.values()) {
      EnumMap<VersionRole, MetricEntityStateBase> innerMap = new EnumMap<>(VersionRole.class);
      for (VersionRole role: VersionRole.values()) {
        if (otelEnabled) {
          Map<VeniceMetricsDimensions, String> dimensionsMap = new HashMap<>(baseDimensionsMap);
          dimensionsMap.put(VeniceMetricsDimensions.VENICE_VERSION_ROLE, role.getDimensionValue());
          dimensionsMap.put(VeniceMetricsDimensions.VENICE_QUOTA_REQUEST_OUTCOME, outcome.getDimensionValue());
          Attributes attributes = baseAttributes.toBuilder()
              .put(
                  otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_VERSION_ROLE),
                  role.getDimensionValue())
              .put(
                  otelRepository.getDimensionName(VeniceMetricsDimensions.VENICE_QUOTA_REQUEST_OUTCOME),
                  outcome.getDimensionValue())
              .build();
          innerMap.put(
              role,
              MetricEntityStateBase
                  .create(metricEntity, otelRepository, Collections.unmodifiableMap(dimensionsMap), attributes));
        } else {
          innerMap.put(role, MetricEntityStateBase.create(metricEntity, otelRepository, baseDimensionsMap, null));
        }
      }
      outerMap.put(outcome, innerMap);
    }
    return outerMap;
  }

  /**
   * Classifies a version number into its current role. Known benign TOCTOU race: the two
   * atomic reads can be stale during version transitions, causing brief misclassification.
   * This is acceptable for metrics — the window is microseconds and version changes are
   * infrequent (once per push). The total count across all roles remains correct.
   */
  private VersionRole classifyVersion(int version) {
    if (version == currentVersion.get()) {
      return VersionRole.CURRENT;
    }
    if (version == backupVersion.get()) {
      return VersionRole.BACKUP;
    }
    return VersionRole.FUTURE;
  }

  public void setCurrentVersion(int version) {
    currentVersion.set(version);
  }

  public void setBackupVersion(int version) {
    backupVersion.set(version);
  }

  public int getCurrentVersion() {
    return currentVersion.get();
  }

  public int getBackupVersion() {
    return backupVersion.get();
  }

  public void removeVersion(int version) {
    versionedStats.remove(version);
  }

  /**
   * @param rcu The number of Read Capacity Units that the allowed request cost
   */
  public void recordAllowed(int version, long rcu) {
    getVersionedStats(version).recordReadQuotaRequested(rcu);
    recordOtelCounters(QuotaRequestOutcome.ALLOWED, version, rcu);
  }

  /**
   * @param rcu The number of Read Capacity Units that the rejected request would have cost
   */
  public void recordRejected(int version, long rcu) {
    getVersionedStats(version).recordReadQuotaRequested(rcu);
    // Tehuti: unversioned sensors
    rejectedQPSSensor.record();
    rejectedKPSSensor.record(rcu);
    recordOtelCounters(QuotaRequestOutcome.REJECTED, version, rcu);
  }

  /**
   * Records requests that were allowed without quota check (quota enforcer not yet initialized
   * or rate limiter not yet allocated for the resource).
   *
   * <p>Note: OTel records both request count and key count for this outcome. Tehuti records only
   * key count (no request count sensor exists for unintentionally allowed requests). This is an
   * intentional enrichment — OTel provides strictly more granularity than Tehuti here.
   *
   * @param version The version number being accessed
   * @param rcu The number of Read Capacity Units that the request cost
   */
  public void recordAllowedUnintentionally(int version, long rcu) {
    allowedUnintentionallyKPSSensor.record(rcu);
    recordOtelCounters(QuotaRequestOutcome.ALLOWED_UNINTENTIONALLY, version, rcu);
  }

  /** Records both request.count and key.count OTel counters for the given outcome and version. */
  private void recordOtelCounters(QuotaRequestOutcome outcome, int version, long rcu) {
    if (!emitOtelMetrics) {
      return;
    }
    VersionRole role = classifyVersion(version);
    requestCountByOutcomeAndRole.get(outcome).get(role).record(1L);
    keyCountByOutcomeAndRole.get(outcome).get(role).record(rcu);
  }

  public void setNodeQuotaResponsibility(int version, long nodeKpsResponsibility) {
    ServerReadQuotaVersionedStats stats = getVersionedStats(version);
    stats.setNodeKpsResponsibility(nodeKpsResponsibility);
  }

  private ServerReadQuotaVersionedStats getVersionedStats(int version) {
    return versionedStats.computeIfAbsent(version, (ignored) -> new ServerReadQuotaVersionedStats(time, metricConfig));
  }

  // Package private for testing purpose
  final Double getVersionedRequestedQPS(int version) {
    if (version < 1) {
      return Double.NaN;
    }
    return getVersionedStats(version).getRequestedQPS();
  }

  // Package private for testing purpose
  final Double getVersionedRequestedKPS(int version) {
    if (version < 1) {
      return Double.NaN;
    }
    return getVersionedStats(version).getRequestedKPS();
  }

  /**
   * @return the ratio of the read quota usage to the node's quota responsibility
   */
  final Double getReadQuotaUsageRatio() {
    int version = currentVersion.get();
    ServerReadQuotaVersionedStats stats = versionedStats.get(version);
    if (version < 1 || stats == null) {
      return Double.NaN;
    }
    long nodeKpsResponsibility = stats.getNodeKpsResponsibility();
    if (nodeKpsResponsibility < 1) {
      return Double.NaN;
    }
    return stats.getRequestedKPS() / nodeKpsResponsibility;
  }

  private static class ServerReadQuotaVersionedStats {
    private final Rate requestedKPS;
    private final Rate requestedQPS;
    private final AtomicLong nodeKpsResponsibility = new AtomicLong(0);
    private final Time time;
    private final MetricConfig metricConfig;

    public ServerReadQuotaVersionedStats(Time time, MetricConfig metricConfig) {
      this.time = time;
      this.metricConfig = metricConfig;
      requestedKPS = new Rate();
      requestedKPS.init(metricConfig, time.getMilliseconds());
      requestedQPS = new Rate();
      requestedQPS.init(metricConfig, time.getMilliseconds());
    }

    public void recordReadQuotaRequested(long rcu) {
      requestedKPS.record(rcu, time.getMilliseconds());
      requestedQPS.record(1, time.getMilliseconds());
    }

    public void setNodeKpsResponsibility(long nodeKpsResponsibility) {
      this.nodeKpsResponsibility.set(nodeKpsResponsibility);
    }

    public double getRequestedKPS() {
      return requestedKPS.measure(metricConfig, time.getMilliseconds());
    }

    public double getRequestedQPS() {
      return requestedQPS.measure(metricConfig, time.getMilliseconds());
    }

    public long getNodeKpsResponsibility() {
      return nodeKpsResponsibility.get();
    }
  }
}
