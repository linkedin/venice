package com.linkedin.davinci.stats;

import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.stats.AbstractVeniceAggVersionedStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

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
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordDuplicateMsg());
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordDuplicateMsg());
  }

  public void recordMissingMsg(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordMissingMsg());
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordMissingMsg());
  }

  public void recordCorruptedMsg(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordCorruptedMsg());
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordCorruptedMsg());
  }

  public void recordSuccessMsg(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordSuccessMsg());
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordSuccessMsg());
  }

  public void recordCurrentIdleTime(String storeName, int version) {
    //we don't record current idle time for total stats
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordCurrentIdleTime());
  }

  public void recordOverallIdleTime(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordOverallIdleTime());
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordOverallIdleTime());
  }

  public void resetCurrentIdleTime(String storeName, int version) {
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.resetCurrentIdleTime());
  }

  public void recordProducerBrokerLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordProducerBrokerLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordProducerBrokerLatencyMs(value));
  }

  public void recordBrokerConsumerLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordBrokerConsumerLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordBrokerConsumerLatencyMs(value));
  }

  public void recordProducerConsumerLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordProducerConsumerLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordProducerConsumerLatencyMs(value));
  }

  public void recordProducerSourceBrokerLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordProducerSourceBrokerLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordProducerSourceBrokerLatencyMs(value));
  }

  public void recordSourceBrokerLeaderConsumerLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordSourceBrokerLeaderConsumerLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordSourceBrokerLeaderConsumerLatencyMs(value));
  }

  public void recordProducerLeaderConsumerLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordProducerLeaderConsumerLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordProducerLeaderConsumerLatencyMs(value));
  }

  public void recordProducerLocalBrokerLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordProducerLocalBrokerLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordProducerLocalBrokerLatencyMs(value));
  }

  public void recordLocalBrokerFollowerConsumerLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordLocalBrokerFollowerConsumerLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordLocalBrokerFollowerConsumerLatencyMs(value));
  }

  public void recordProducerFollowerConsumerLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordProducerFollowerConsumerLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordProducerFollowerConsumerLatencyMs(value));
  }

  public void recordDataValidationLatencyMs(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordDataValidationLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordDataValidationLatencyMs(value));
  }

  public void recordBenignLeaderOffsetRewind(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordBenignLeaderOffsetRewind());
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordBenignLeaderOffsetRewind());
  }

  public void recordPotentiallyLossyLeaderOffsetRewind(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordPotentiallyLossyLeaderOffsetRewind());
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordPotentiallyLossyLeaderOffsetRewind());
  }

  public void recordLeaderProducerFailure(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordLeaderProducerFailure());
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordLeaderProducerFailure());
  }

  public void recordBenignLeaderProducerFailure(String storeName, int version) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordBenignLeaderProducerFailure());
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordBenignLeaderProducerFailure());
  }

  public void recordLeaderProducerCompletionTime(String storeName, int version, double value) {
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> stat.recordLeaderProducerCompletionLatencyMs(value));
    Utils.computeIfNotNull(getStats(storeName, version), stat -> stat.recordLeaderProducerCompletionLatencyMs(value));
  }

  @Override
  protected void updateTotalStats(String storeName) {
    Set<Integer> existingVersions = new HashSet<>();
    existingVersions.addAll(Arrays.asList(getBackupVersion(storeName), getCurrentVersion(storeName), getFutureVersion(storeName)));
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

  private void resetTotalStats(String storeName, Set<Integer> existingVersions, Function<DIVStats, Long> statValueSupplier, BiConsumer<DIVStats, Long> statsUpdater) {
    AtomicLong totalStatCount = new AtomicLong(0L);
    existingVersions.forEach(v -> Utils.computeIfNotNull(getStats(storeName, v),
        stat -> totalStatCount.addAndGet(statValueSupplier.apply(stat))));
    Utils.computeIfNotNull(getTotalStats(storeName), stat -> statsUpdater.accept(stat, totalStatCount.get()));
  }
}
