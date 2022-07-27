package com.linkedin.davinci.stats;

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

import static com.linkedin.venice.meta.Store.*;


public class AggVersionedDIVStats extends AbstractVeniceAggVersionedStats<DIVStats, DIVStatsReporter> {
  public AggVersionedDIVStats(MetricsRepository metricsRepository, ReadOnlyStoreRepository metadataRepository) {
    super(metricsRepository, metadataRepository, () -> new DIVStats(), (mr, name) -> new DIVStatsReporter(mr, name));
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

  public void recordCurrentIdleTime(String storeName, int version) {
    //we don't record current idle time for total stats
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordCurrentIdleTime());
  }

  public void recordOverallIdleTime(String storeName, int version) {
    recordVersionedAndTotalStat(storeName, version, DIVStats::recordOverallIdleTime);
  }

  public void resetCurrentIdleTime(String storeName, int version) {
    //we don't record current idle time for total stats
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.resetCurrentIdleTime());
  }

  public void recordLatencies(
      String storeName,
      int version,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs,
      double producerConsumerLatencyMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerBrokerLatencyMs(producerBrokerLatencyMs);
      stat.recordBrokerConsumerLatencyMs(brokerConsumerLatencyMs);
      stat.recordProducerConsumerLatencyMs(producerConsumerLatencyMs);
    });
  }

  public void recordLeaderLatencies(
      String storeName,
      int version,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs,
      double producerConsumerLatencyMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerSourceBrokerLatencyMs(producerBrokerLatencyMs);
      stat.recordSourceBrokerLeaderConsumerLatencyMs(brokerConsumerLatencyMs);
      stat.recordProducerLeaderConsumerLatencyMs(producerConsumerLatencyMs);
    });
  }

  public void recordFollowerLatencies(
      String storeName,
      int version,
      double producerBrokerLatencyMs,
      double brokerConsumerLatencyMs,
      double producerConsumerLatencyMs) {
    recordVersionedAndTotalStat(storeName, version, stat -> {
      stat.recordProducerLocalBrokerLatencyMs(producerBrokerLatencyMs);
      stat.recordLocalBrokerFollowerConsumerLatencyMs(brokerConsumerLatencyMs);
      stat.recordProducerFollowerConsumerLatencyMs(producerConsumerLatencyMs);
    });
  }

  public void recordDataValidationLatencyMs(String storeName, int version, double value) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordDataValidationLatencyMs(value));
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

  public void recordLeaderProducerCompletionTime(String storeName, int version, double value) {
    recordVersionedAndTotalStat(storeName, version, stat -> stat.recordLeaderProducerCompletionLatencyMs(value));
  }

  @Override
  protected void updateTotalStats(String storeName) {
    IntSet existingVersions = new IntOpenHashSet(3);
    existingVersions.add(getBackupVersion(storeName));
    existingVersions.add(getCurrentVersion(storeName));
    existingVersions.add(getFutureVersion(storeName));
    existingVersions.remove(NON_EXISTING_VERSION);

    // Update total producer failure count
    resetTotalStats(storeName, existingVersions, stat -> stat.getLeaderProducerFailure(),
        (stat, count) -> stat.setLeaderProducerFailure(count));
    // Update total benign leader producer failure count
    resetTotalStats(storeName, existingVersions, stat -> stat.getBenignLeaderProducerFailure(),
        (stat, count) -> stat.setBenignLeaderProducerFailure(count));
    // Update total benign leader offset rewind count
    resetTotalStats(storeName, existingVersions, stat -> stat.getBenignLeaderOffsetRewindCount(),
        (stat, count) -> stat.setBenignLeaderOffsetRewindCount(count));
    // Update total potentially lossy leader offset rewind count
    resetTotalStats(storeName, existingVersions, stat -> stat.getPotentiallyLossyLeaderOffsetRewindCount(),
        (stat, count) -> stat.setPotentiallyLossyLeaderOffsetRewindCount(count));
    // Update total duplicated msg count
    resetTotalStats(storeName, existingVersions, stat -> stat.getDuplicateMsg(),
        (stat, count) -> stat.setDuplicateMsg(count));
    // Update total missing msg count
    resetTotalStats(storeName, existingVersions, stat -> stat.getMissingMsg(),
        (stat, count) -> stat.setMissingMsg(count));
    // Update total corrupt msg count
    resetTotalStats(storeName, existingVersions, stat -> stat.getCorruptedMsg(),
        (stat, count) -> stat.setCorruptedMsg(count));
  }

  private void resetTotalStats(String storeName, IntSet existingVersions, Function<DIVStats, Long> statValueSupplier, BiConsumer<DIVStats, Long> statsUpdater) {
    AtomicLong totalStatCount = new AtomicLong(0L);
    IntConsumer versionConsumer = v -> Utils.computeIfNotNull(
        getStats(storeName, v), stat -> totalStatCount.addAndGet(statValueSupplier.apply(stat)));
    existingVersions.forEach(versionConsumer);
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> statsUpdater.accept(stat, totalStatCount.get()));
  }
}
