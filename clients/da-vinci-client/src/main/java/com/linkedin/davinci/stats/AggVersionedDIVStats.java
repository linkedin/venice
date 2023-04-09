package com.linkedin.davinci.stats;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;


public class AggVersionedDIVStats extends AbstractVeniceAggVersionedStats<DIVStats, DIVStatsReporter> {
  public AggVersionedDIVStats(
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled) {
    super(
        metricsRepository,
        metadataRepository,
        DIVStats::new,
        DIVStatsReporter::new,
        unregisterMetricForDeletedStoreEnabled);
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
  }

  public void recordMissingMsg(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordMissingMsg);
  }

  public void recordCorruptedMsg(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordCorruptedMsg);
  }

  public void recordSuccessMsg(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordSuccessMsg);
  }

  public void recordLatencies(
      String storeName,
      int version,
      long currentTimeMs,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs,
      double producerConsumerLatencyMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerBrokerLatencyMs(producerBrokerLatencyMs, currentTimeMs);
      stat.recordBrokerConsumerLatencyMs(brokerConsumerLatencyMs, currentTimeMs);
      stat.recordProducerConsumerLatencyMs(producerConsumerLatencyMs, currentTimeMs);
    });
  }

  public void recordLeaderLatencies(
      String storeName,
      int version,
      long currentTimeMs,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs,
      double producerConsumerLatencyMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerSourceBrokerLatencyMs(producerBrokerLatencyMs, currentTimeMs);
      stat.recordSourceBrokerLeaderConsumerLatencyMs(brokerConsumerLatencyMs, currentTimeMs);
      stat.recordProducerLeaderConsumerLatencyMs(producerConsumerLatencyMs, currentTimeMs);
    });
  }

  public void recordFollowerLatencies(
      String storeName,
      int version,
      long currentTimeMs,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs,
      double producerConsumerLatencyMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerLocalBrokerLatencyMs(producerBrokerLatencyMs, currentTimeMs);
      stat.recordLocalBrokerFollowerConsumerLatencyMs(brokerConsumerLatencyMs, currentTimeMs);
      stat.recordProducerFollowerConsumerLatencyMs(producerConsumerLatencyMs, currentTimeMs);
    });
  }

  public void recordLeaderProducerCompletionTime(String storeName, int version, double value, long currentTimeMs) {
    recordVersionedAndTotalStat(
        storeName,
        version,
        stat -> stat.recordLeaderProducerCompletionLatencyMs(value, currentTimeMs));
  }

  public void recordBenignLeaderOffsetRewind(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordBenignLeaderOffsetRewind);
  }

  public void recordPotentiallyLossyLeaderOffsetRewind(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordPotentiallyLossyLeaderOffsetRewind);
  }

  public void recordLeaderProducerFailure(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordLeaderProducerFailure);
  }

  public void recordBenignLeaderProducerFailure(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordBenignLeaderProducerFailure);
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
}
