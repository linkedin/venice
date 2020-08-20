package com.linkedin.venice.stats;

import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import io.tehuti.metrics.MetricsRepository;

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
    getTotalStats(storeName).recordDuplicateMsg();
    getStats(storeName, version).recordDuplicateMsg();
  }

  public void recordMissingMsg(String storeName, int version) {
    getTotalStats(storeName).recordMissingMsg();
    getStats(storeName, version).recordMissingMsg();
  }

  public void recordCorruptedMsg(String storeName, int version) {
    getTotalStats(storeName).recordCorruptedMsg();
    getStats(storeName, version).recordCorruptedMsg();
  }

  public void recordSuccessMsg(String storeName, int version) {
    getTotalStats(storeName).recordSuccessMsg();
    getStats(storeName, version).recordSuccessMsg();
  }

  public void recordCurrentIdleTime(String storeName, int version) {
    //we don't record current idle time for total stats
    getStats(storeName, version).recordCurrentIdleTime();
  }

  public void recordOverallIdleTime(String storeName, int version) {
    getTotalStats(storeName).recordOverallIdleTime();
    getStats(storeName, version).recordOverallIdleTime();
  }

  public void resetCurrentIdleTime(String storeName, int version) {
    getStats(storeName, version).resetCurrentIdleTime();
  }

  public void recordProducerBrokerLatencyMs(String storeName, int version, double value) {
    getTotalStats(storeName).recordProducerBrokerLatencyMs(value);
    getStats(storeName, version).recordProducerBrokerLatencyMs(value);
  }

  public void recordBrokerConsumerLatencyMs(String storeName, int version, double value) {
    getTotalStats(storeName).recordBrokerConsumerLatencyMs(value);
    getStats(storeName, version).recordBrokerConsumerLatencyMs(value);
  }

  public void recordProducerConsumerLatencyMs(String storeName, int version, double value) {
    getTotalStats(storeName).recordProducerConsumerLatencyMs(value);
    getStats(storeName, version).recordProducerConsumerLatencyMs(value);
  }

  public void recordProducerSourceBrokerLatencyMs(String storeName, int version, double value) {
    getTotalStats(storeName).recordProducerSourceBrokerLatencyMs(value);
    getStats(storeName, version).recordProducerSourceBrokerLatencyMs(value);
  }

  public void recordSourceBrokerLeaderConsumerLatencyMs(String storeName, int version, double value) {
    getTotalStats(storeName).recordSourceBrokerLeaderConsumerLatencyMs(value);
    getStats(storeName, version).recordSourceBrokerLeaderConsumerLatencyMs(value);
  }

  public void recordProducerLeaderConsumerLatencyMs(String storeName, int version, double value) {
    getTotalStats(storeName).recordProducerLeaderConsumerLatencyMs(value);
    getStats(storeName, version).recordProducerLeaderConsumerLatencyMs(value);
  }

  public void recordProducerLocalBrokerLatencyMs(String storeName, int version, double value) {
    getTotalStats(storeName).recordProducerLocalBrokerLatencyMs(value);
    getStats(storeName, version).recordProducerLocalBrokerLatencyMs(value);
  }

  public void recordLocalBrokerFollowerConsumerLatencyMs(String storeName, int version, double value) {
    getTotalStats(storeName).recordLocalBrokerFollowerConsumerLatencyMs(value);
    getStats(storeName, version).recordLocalBrokerFollowerConsumerLatencyMs(value);
  }

  public void recordProducerFollowerConsumerLatencyMs(String storeName, int version, double value) {
    getTotalStats(storeName).recordProducerFollowerConsumerLatencyMs(value);
    getStats(storeName, version).recordProducerFollowerConsumerLatencyMs(value);
  }

  public void recordBenignLeaderOffsetRewind(String storeName, int version) {
    getTotalStats(storeName).recordBenignLeaderOffsetRewind();
    getStats(storeName, version).recordBenignLeaderOffsetRewind();
  }

  public void recordPotentiallyLossyLeaderOffsetRewind(String storeName, int version) {
    getTotalStats(storeName).recordPotentiallyLossyLeaderOffsetRewind();
    getStats(storeName, version).recordPotentiallyLossyLeaderOffsetRewind();
  }

  public void recordLeaderProducerFailure(String storeName, int version) {
    getTotalStats(storeName).recordLeaderProducerFailure();
    getStats(storeName, version).recordLeaderProducerFailure();
  }
}
