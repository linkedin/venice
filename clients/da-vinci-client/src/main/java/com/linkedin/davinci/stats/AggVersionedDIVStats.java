package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.OtelVersionedStatsUtils.classifyVersion;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
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
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import com.linkedin.venice.stats.metrics.MetricScope;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
 *
 * <p><b>OTel lifecycle:</b> a store's OTel metrics are closed when its last ingestion task on this host stops or
 * when the store is deleted.
 */
public class AggVersionedDIVStats extends AbstractVeniceAggVersionedStats<DIVStats, DIVStatsReporter> {
  private static final Logger LOGGER = LogManager.getLogger(AggVersionedDIVStats.class);
  private final boolean emitOtelMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final PerStoreVersionedOtelStats<DIVOtelStats> otelStats;

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
    this.otelStats = createPerStoreOtelStats(DIVOtelStats::new);
  }

  /** Registers a running ingestion task so the store's OTel metrics stay open until it stops. Never throws. */
  public void setIngestionTask(String storeName, StoreIngestionTask ingestionTask) {
    if (!emitOtelMetrics) {
      return;
    }
    try {
      otelStats.compute(storeName, stats -> stats.ingestionTasks.add(ingestionTask));
    } catch (Exception e) {
      LOGGER.warn("Failed to attach ingestion task to DIV OTel stats of store: {}", storeName, e);
    }
  }

  /** Unregisters a stopped ingestion task and closes the store's OTel metrics once none is left. Never throws. */
  public void removeIngestionTask(String storeName, StoreIngestionTask ingestionTask) {
    try {
      otelStats.removeIf(storeName, stats -> {
        stats.ingestionTasks.remove(ingestionTask);
        return stats.ingestionTasks.isEmpty();
      });
    } catch (Exception e) {
      LOGGER.warn("Failed to detach ingestion task from DIV OTel stats of store: {}", storeName, e);
    }
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
    recordOtelFailureCount(storeName, version, stats -> stats.producerFailureCount);
  }

  public void recordBenignLeaderProducerFailure(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordBenignLeaderProducerFailure);
    recordOtelFailureCount(storeName, version, stats -> stats.benignProducerFailureCount);
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

  private Map<VeniceMetricsDimensions, String> buildStoreDimensionsMap(String storeName) {
    Map<VeniceMetricsDimensions, String> map = new HashMap<>(baseDimensionsMap);
    map.put(VENICE_STORE_NAME, OpenTelemetryMetricsSetup.sanitizeStoreName(storeName));
    return Collections.unmodifiableMap(map);
  }

  private void recordOtelMessageCount(String storeName, int version, VeniceDIVResult result) {
    if (emitOtelMetrics) {
      DIVOtelStats stats = otelStats.getOrCreate(storeName);
      stats.messageCount.record(1, stats.classify(version), result);
    }
  }

  private void recordOtelOffsetRewindCount(String storeName, int version, VeniceDIVSeverity severity) {
    if (emitOtelMetrics) {
      DIVOtelStats stats = otelStats.getOrCreate(storeName);
      stats.offsetRewindCount.record(1, stats.classify(version), severity);
    }
  }

  private void recordOtelFailureCount(
      String storeName,
      int version,
      Function<DIVOtelStats, MetricEntityStateOneEnum<VersionRole>> metric) {
    if (emitOtelMetrics) {
      DIVOtelStats stats = otelStats.getOrCreate(storeName);
      metric.apply(stats).record(1, stats.classify(version));
    }
  }

  /** One store's DIV OTel metrics and the running ingestion tasks that keep them open. */
  private final class DIVOtelStats implements StoreOtelStats {
    private final MetricScope metricScope = new MetricScope();
    private final Set<StoreIngestionTask> ingestionTasks = VeniceConcurrentHashMap.newKeySet();
    private final MetricEntityStateTwoEnums<VersionRole, VeniceDIVResult> messageCount;
    private final MetricEntityStateTwoEnums<VersionRole, VeniceDIVSeverity> offsetRewindCount;
    private final MetricEntityStateOneEnum<VersionRole> producerFailureCount;
    private final MetricEntityStateOneEnum<VersionRole> benignProducerFailureCount;
    private volatile VersionInfo versionInfo = VersionInfo.NON_EXISTING;

    private DIVOtelStats(String storeName) {
      Map<VeniceMetricsDimensions, String> dimensions = buildStoreDimensionsMap(storeName);
      this.messageCount = metricScope.register(
          MetricEntityStateTwoEnums.create(
              DIVOtelMetricEntity.MESSAGE_COUNT.getMetricEntity(),
              otelRepository,
              dimensions,
              VersionRole.class,
              VeniceDIVResult.class));
      this.offsetRewindCount = metricScope.register(
          MetricEntityStateTwoEnums.create(
              DIVOtelMetricEntity.OFFSET_REWIND_COUNT.getMetricEntity(),
              otelRepository,
              dimensions,
              VersionRole.class,
              VeniceDIVSeverity.class));
      this.producerFailureCount = metricScope.register(
          MetricEntityStateOneEnum.create(
              DIVOtelMetricEntity.PRODUCER_FAILURE_COUNT.getMetricEntity(),
              otelRepository,
              dimensions,
              VersionRole.class));
      this.benignProducerFailureCount = metricScope.register(
          MetricEntityStateOneEnum.create(
              DIVOtelMetricEntity.BENIGN_PRODUCER_FAILURE_COUNT.getMetricEntity(),
              otelRepository,
              dimensions,
              VersionRole.class));
    }

    private VersionRole classify(int version) {
      return classifyVersion(version, versionInfo);
    }

    @Override
    public void updateVersionInfo(int currentVersion, int futureVersion) {
      versionInfo = new VersionInfo(currentVersion, futureVersion);
    }

    @Override
    public void close() {
      metricScope.close();
    }
  }
}
