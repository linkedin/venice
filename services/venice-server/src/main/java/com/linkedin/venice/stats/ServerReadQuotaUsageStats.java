package com.linkedin.venice.stats;

import com.linkedin.davinci.stats.ServerReadQuotaOtelMetricEntity;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.dimensions.QuotaRequestOutcome;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
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
import java.util.Map;
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
   * Immutable holder for current and backup version numbers, following the same volatile +
   * immutable pattern as {@link com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo}.
   *
   * <p>Unlike {@code OtelVersionedStatsUtils.VersionInfo} which tracks current + future (where
   * unknown versions default to BACKUP), quota enforcement tracks current + backup (where unknown
   * versions default to FUTURE). This is because quota rate limiters are only allocated for
   * current and backup versions — an unrecognized version is a future version arriving before
   * its rate limiter is set up.
   *
   * @see #classifyVersion(int, QuotaVersionInfo)
   */
  static class QuotaVersionInfo {
    final int currentVersion;
    final int backupVersion;

    QuotaVersionInfo(int currentVersion, int backupVersion) {
      this.currentVersion = currentVersion;
      this.backupVersion = backupVersion;
    }
  }

  /**
   * Classifies a version as CURRENT, BACKUP, or FUTURE for quota metrics.
   *
   * <p>This is analogous to {@link com.linkedin.davinci.stats.OtelVersionedStatsUtils#classifyVersion}
   * but with inverted default: unknown versions are FUTURE (not BACKUP) because quota enforcement
   * only tracks current and backup — any other version is a future version whose rate limiter
   * has not been allocated yet.
   *
   * @param version The version number to classify
   * @param versionInfo The current/backup version info snapshot
   * @return {@link VersionRole#CURRENT} if version matches currentVersion,
   *         {@link VersionRole#BACKUP} if version matches backupVersion,
   *         {@link VersionRole#FUTURE} otherwise
   */
  static VersionRole classifyVersion(int version, QuotaVersionInfo versionInfo) {
    if (version == versionInfo.currentVersion) {
      return VersionRole.CURRENT;
    } else if (version == versionInfo.backupVersion) {
      return VersionRole.BACKUP;
    }
    return VersionRole.FUTURE;
  }

  /** OTel high-perf counter with QuotaRequestOutcome × VersionRole dimensions. */
  private final MetricEntityStateTwoEnums<QuotaRequestOutcome, VersionRole> requestCount;
  /** OTel high-perf counter with QuotaRequestOutcome × VersionRole dimensions. */
  private final MetricEntityStateTwoEnums<QuotaRequestOutcome, VersionRole> keyCount;

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
  private volatile QuotaVersionInfo versionInfo =
      new QuotaVersionInfo(Store.NON_EXISTING_VERSION, Store.NON_EXISTING_VERSION);
  private final Time time;
  private final MetricConfig metricConfig;

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
            (ignored, ignored2) -> getVersionedRequestedQPS(versionInfo.currentVersion),
            TehutiMetricName.CURRENT_QUOTA_REQUEST.getMetricName()));
    registerSensor(
        TehutiMetricName.BACKUP_QUOTA_REQUEST.getMetricName(),
        new AsyncGauge(
            (ignored, ignored2) -> getVersionedRequestedQPS(versionInfo.backupVersion),
            TehutiMetricName.BACKUP_QUOTA_REQUEST.getMetricName()));
    registerSensor(
        TehutiMetricName.CURRENT_QUOTA_REQUEST_KEY_COUNT.getMetricName(),
        new AsyncGauge(
            (ignored, ignored2) -> getVersionedRequestedKPS(versionInfo.currentVersion),
            TehutiMetricName.CURRENT_QUOTA_REQUEST_KEY_COUNT.getMetricName()));
    registerSensor(
        TehutiMetricName.BACKUP_QUOTA_REQUEST_KEY_COUNT.getMetricName(),
        new AsyncGauge(
            (ignored, ignored2) -> getVersionedRequestedKPS(versionInfo.backupVersion),
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
    requestCount = MetricEntityStateTwoEnums.create(
        ServerReadQuotaOtelMetricEntity.READ_QUOTA_REQUEST_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        QuotaRequestOutcome.class,
        VersionRole.class);
    keyCount = MetricEntityStateTwoEnums.create(
        ServerReadQuotaOtelMetricEntity.READ_QUOTA_KEY_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        QuotaRequestOutcome.class,
        VersionRole.class);
  }

  /**
   * Atomically updates both current and backup version numbers. This replaces the volatile
   * {@link QuotaVersionInfo} reference in a single write, matching the pattern used by
   * {@link com.linkedin.davinci.stats.ingestion.IngestionOtelStats#updateVersionInfo}.
   */
  public void updateVersionInfo(int currentVersion, int backupVersion) {
    this.versionInfo = new QuotaVersionInfo(currentVersion, backupVersion);
  }

  public int getCurrentVersion() {
    return versionInfo.currentVersion;
  }

  public int getBackupVersion() {
    return versionInfo.backupVersion;
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
    VersionRole role = classifyVersion(version, versionInfo);
    requestCount.record(1L, outcome, role);
    keyCount.record(rcu, outcome, role);
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
    int version = versionInfo.currentVersion;
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
