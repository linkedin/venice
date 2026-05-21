package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.OtelVersionedStatsUtils.classifyVersion;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDIVResult;
import com.linkedin.venice.stats.dimensions.VeniceDIVSeverity;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AbstractStatsCloseable;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import com.linkedin.venice.stats.metrics.MetricEntityStateUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;


/**
 * Aggregated versioned DIV stats with dual Tehuti + OTel recording.
 *
 * <p><b>Recording architecture:</b> Each public recording method (e.g., {@link #recordSuccessMsg})
 * records to <b>both</b> Tehuti (via {@code recordVersionedAndTotalStat} into total + per-version
 * {@link DIVStats} objects) and OTel (once per call, with store/cluster/version-role dimensions).
 * OTel totals are derived at query time by aggregating across the version-role dimension —
 * no separate OTel recording for total stats.
 *
 * <p><b>Version classification:</b> The version number passed to each recording method is classified
 * as CURRENT, FUTURE, or BACKUP for the OTel {@code VERSION_ROLE} dimension. Versions not matching
 * the registered current or future version default to BACKUP.
 */
public class AggVersionedDIVStats extends AbstractVeniceAggVersionedStats<DIVStats, DIVStatsReporter> {
  private final boolean emitOtelMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  /**
   * Per-store entry map. Each entry bundles the per-store {@link AbstractStatsCloseable#resources}
   * with lazily-populated wrappers for the four per-store OTel metrics. A single {@code remove()} in
   * {@link #handleStoreDeleted(String)} closes all wrappers atomically — there is no
   * cross-map race window where a concurrent record could resurrect parallel entries.
   *
   * <p>Map grows lazily via {@code computeIfAbsent} and is bounded by the number of stores the
   * server is actively ingesting. Each map is OTel-only; Tehuti recording is handled by the parent
   * class via {@code recordVersionedAndTotalStat}.
   */
  private final Map<String, PerStoreEntry> perStore = new VeniceConcurrentHashMap<>();

  /** Per-store state held by {@link #perStore}. */
  private static final class PerStoreEntry extends AbstractStatsCloseable {
    final MetricEntityStateTwoEnums<VersionRole, VeniceDIVResult> messageCount;
    final MetricEntityStateTwoEnums<VersionRole, VeniceDIVSeverity> offsetRewindCount;
    final MetricEntityStateOneEnum<VersionRole> producerFailureCount;
    final MetricEntityStateOneEnum<VersionRole> benignProducerFailureCount;

    PerStoreEntry(VeniceOpenTelemetryMetricsRepository otelRepository, Map<VeniceMetricsDimensions, String> dims) {
      this.messageCount = MetricEntityStateTwoEnums.create(
          DIVOtelMetricEntity.MESSAGE_COUNT.getMetricEntity(),
          otelRepository,
          dims,
          VersionRole.class,
          VeniceDIVResult.class,
          statsCloseables);
      this.offsetRewindCount = MetricEntityStateTwoEnums.create(
          DIVOtelMetricEntity.OFFSET_REWIND_COUNT.getMetricEntity(),
          otelRepository,
          dims,
          VersionRole.class,
          VeniceDIVSeverity.class,
          statsCloseables);
      this.producerFailureCount = MetricEntityStateOneEnum.create(
          DIVOtelMetricEntity.PRODUCER_FAILURE_COUNT.getMetricEntity(),
          otelRepository,
          dims,
          VersionRole.class,
          statsCloseables);
      this.benignProducerFailureCount = MetricEntityStateOneEnum.create(
          DIVOtelMetricEntity.BENIGN_PRODUCER_FAILURE_COUNT.getMetricEntity(),
          otelRepository,
          dims,
          VersionRole.class,
          statsCloseables);
    }
  }

  /**
   * Per-store version info for classifying versions as CURRENT, FUTURE, or BACKUP.
   * Updated via {@link #onVersionInfoUpdated(String, int, int)}. This map is intentionally
   * separate from {@link #perStore}: it stores plain data (no {@link java.io.Closeable}), so it
   * is not vulnerable to the delete-vs-record resurrection race that motivated bundling the
   * per-store closeable wrappers into a single entry holder.
   */
  private final Map<String, OtelVersionedStatsUtils.VersionInfo> versionInfoMap = new VeniceConcurrentHashMap<>();

  public AggVersionedDIVStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled,
      String clusterName) {
    super(
        metricsRepository,
        metadataRepository,
        DIVStats::new,
        DIVStatsReporter::new,
        unregisterMetricForDeletedStoreEnabled);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    this.emitOtelMetrics = otelData.emitOpenTelemetryMetrics();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = Collections.unmodifiableMap(otelData.getBaseDimensionsMap());
  }

  public void recordException(String storeName, int version, DataValidationException e) {
    if (e instanceof DuplicateDataException) {
      recordDuplicateMsg(storeName, version);
    } else if (e instanceof MissingDataException) {
      recordMissingMsg(storeName, version);
    } else if (e instanceof CorruptDataException) {
      recordCorruptedMsg(storeName, version);
    }
  }

  public void recordDuplicateMsg(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordDuplicateMsg);
    recordOtelMessageCount(storeName, version, VeniceDIVResult.DUPLICATE);
  }

  public void recordMissingMsg(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordMissingMsg);
    recordOtelMessageCount(storeName, version, VeniceDIVResult.MISSING);
  }

  public void recordCorruptedMsg(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordCorruptedMsg);
    recordOtelMessageCount(storeName, version, VeniceDIVResult.CORRUPTED);
  }

  public void recordSuccessMsg(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordSuccessMsg);
    recordOtelMessageCount(storeName, version, VeniceDIVResult.SUCCESS);
  }

  public void recordBenignLeaderOffsetRewind(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordBenignLeaderOffsetRewind);
    recordOtelOffsetRewindCount(storeName, version, VeniceDIVSeverity.BENIGN);
  }

  public void recordPotentiallyLossyLeaderOffsetRewind(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordPotentiallyLossyLeaderOffsetRewind);
    recordOtelOffsetRewindCount(storeName, version, VeniceDIVSeverity.POTENTIALLY_LOSSY);
  }

  public void recordLeaderProducerFailure(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordLeaderProducerFailure);
    recordOtelProducerFailureCount(storeName, version, false);
  }

  public void recordBenignLeaderProducerFailure(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordBenignLeaderProducerFailure);
    recordOtelProducerFailureCount(storeName, version, true);
  }

  /** {@link AbstractVeniceAggVersionedStats#addStore(com.linkedin.venice.meta.Store)}
   *  calls this from the super() constructor before {@code versionInfoMap} is initialized. */
  @Override
  protected void onVersionInfoUpdated(String storeName, int currentVersion, int futureVersion) {
    if (versionInfoMap == null) {
      return; // Called during super() constructor before versionInfoMap is initialized
    }
    versionInfoMap.put(storeName, new OtelVersionedStatsUtils.VersionInfo(currentVersion, futureVersion));
  }

  @Override
  public void handleStoreDeleted(String storeName) {
    try {
      super.handleStoreDeleted(storeName);
    } finally {
      MetricEntityStateUtils.closeQuietly(perStore.remove(storeName));
      versionInfoMap.remove(storeName);
    }
  }

  @Override
  protected void updateTotalStats(String storeName) {
    IntSet existingVersions = new IntOpenHashSet(3);
    existingVersions.add(getCurrentVersion(storeName));
    existingVersions.add(getFutureVersion(storeName));
    existingVersions.remove(NON_EXISTING_VERSION);

    // Update total producer failure count
    resetTotalStats(
        storeName,
        existingVersions,
        DIVStats::getLeaderProducerFailure,
        DIVStats::setLeaderProducerFailure);
    // Update total benign leader producer failure count
    resetTotalStats(
        storeName,
        existingVersions,
        DIVStats::getBenignLeaderProducerFailure,
        DIVStats::setBenignLeaderProducerFailure);
    // Update total benign leader offset rewind count
    resetTotalStats(
        storeName,
        existingVersions,
        DIVStats::getBenignLeaderOffsetRewindCount,
        DIVStats::setBenignLeaderOffsetRewindCount);
    // Update total potentially lossy leader offset rewind count
    resetTotalStats(
        storeName,
        existingVersions,
        DIVStats::getPotentiallyLossyLeaderOffsetRewindCount,
        DIVStats::setPotentiallyLossyLeaderOffsetRewindCount);
    // Update total duplicated msg count
    resetTotalStats(storeName, existingVersions, DIVStats::getDuplicateMsg, DIVStats::setDuplicateMsg);
    // Update total missing msg count
    resetTotalStats(storeName, existingVersions, DIVStats::getMissingMsg, DIVStats::setMissingMsg);
    // Update total corrupt msg count
    resetTotalStats(storeName, existingVersions, DIVStats::getCorruptedMsg, DIVStats::setCorruptedMsg);
    // Update total success msg count
    resetTotalStats(storeName, existingVersions, DIVStats::getSuccessMsg, DIVStats::setSuccessMsg);
  }

  private void resetTotalStats(
      String storeName,
      IntSet existingVersions,
      Function<DIVStats, Long> statValueSupplier,
      BiConsumer<DIVStats, Long> statsUpdater) {
    AtomicLong totalStatCount = new AtomicLong(0L);
    IntConsumer versionConsumer = v -> Utils
        .computeIfNotNull(getStats(storeName, v), stat -> totalStatCount.addAndGet(statValueSupplier.apply(stat)));
    existingVersions.forEach(versionConsumer);
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> statsUpdater.accept(stat, totalStatCount.get()));
  }

  // --- OTel recording helpers ---

  private PerStoreEntry getOrCreateEntry(String storeName) {
    return perStore.computeIfAbsent(
        storeName,
        k -> new PerStoreEntry(
            otelRepository,
            OpenTelemetryMetricsSetup.buildStoreDimensionsMap(baseDimensionsMap, k)));
  }

  private void recordOtelMessageCount(String storeName, int version, VeniceDIVResult result) {
    if (!emitOtelMetrics) {
      return;
    }
    VersionRole role = classifyVersion(version, versionInfoMap.get(storeName));
    getOrCreateEntry(storeName).messageCount.record(1, role, result);
  }

  private void recordOtelOffsetRewindCount(String storeName, int version, VeniceDIVSeverity severity) {
    if (!emitOtelMetrics) {
      return;
    }
    VersionRole role = classifyVersion(version, versionInfoMap.get(storeName));
    getOrCreateEntry(storeName).offsetRewindCount.record(1, role, severity);
  }

  private void recordOtelProducerFailureCount(String storeName, int version, boolean benign) {
    if (!emitOtelMetrics) {
      return;
    }
    VersionRole role = classifyVersion(version, versionInfoMap.get(storeName));
    PerStoreEntry entry = getOrCreateEntry(storeName);
    (benign ? entry.benignProducerFailureCount : entry.producerFailureCount).record(1, role);
  }

  @Override
  public void close() {
    // Unregister metadata listener first so handleStore* can't re-populate the maps while we drain.
    super.close();
    MetricEntityStateUtils.closeAndClear(perStore);
    versionInfoMap.clear();
  }
}
