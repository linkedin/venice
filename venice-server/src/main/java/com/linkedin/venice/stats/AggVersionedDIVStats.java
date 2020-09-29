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
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordDuplicateMsg();
    if(getStats(storeName, version) != null) getStats(storeName, version).recordDuplicateMsg();
  }

  public void recordMissingMsg(String storeName, int version) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordMissingMsg();
    if(getStats(storeName, version) != null) getStats(storeName, version).recordMissingMsg();
  }

  public void recordCorruptedMsg(String storeName, int version) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordCorruptedMsg();
    if(getStats(storeName, version) != null) getStats(storeName, version).recordCorruptedMsg();
  }

  public void recordSuccessMsg(String storeName, int version) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordSuccessMsg();
    if(getStats(storeName, version) != null) getStats(storeName, version).recordSuccessMsg();
  }

  public void recordCurrentIdleTime(String storeName, int version) {
    //we don't record current idle time for total stats
    if(getStats(storeName, version) != null) getStats(storeName, version).recordCurrentIdleTime();
  }

  public void recordOverallIdleTime(String storeName, int version) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordOverallIdleTime();
    if(getStats(storeName, version) != null) getStats(storeName, version).recordOverallIdleTime();
  }

  public void resetCurrentIdleTime(String storeName, int version) {
    if(getStats(storeName, version) != null) getStats(storeName, version).resetCurrentIdleTime();
  }

  public void recordProducerBrokerLatencyMs(String storeName, int version, double value) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordProducerBrokerLatencyMs(value);
    if(getStats(storeName, version) != null) getStats(storeName, version).recordProducerBrokerLatencyMs(value);
  }

  public void recordBrokerConsumerLatencyMs(String storeName, int version, double value) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordBrokerConsumerLatencyMs(value);
    if(getStats(storeName, version) != null) getStats(storeName, version).recordBrokerConsumerLatencyMs(value);
  }

  public void recordProducerConsumerLatencyMs(String storeName, int version, double value) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordProducerConsumerLatencyMs(value);
    if(getStats(storeName, version) != null) getStats(storeName, version).recordProducerConsumerLatencyMs(value);
  }

  public void recordProducerSourceBrokerLatencyMs(String storeName, int version, double value) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordProducerSourceBrokerLatencyMs(value);
    if(getStats(storeName, version) != null) getStats(storeName, version).recordProducerSourceBrokerLatencyMs(value);
  }

  public void recordSourceBrokerLeaderConsumerLatencyMs(String storeName, int version, double value) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordSourceBrokerLeaderConsumerLatencyMs(value);
    if(getStats(storeName, version) != null) getStats(storeName, version).recordSourceBrokerLeaderConsumerLatencyMs(value);
  }

  public void recordProducerLeaderConsumerLatencyMs(String storeName, int version, double value) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordProducerLeaderConsumerLatencyMs(value);
    if(getStats(storeName, version) != null) getStats(storeName, version).recordProducerLeaderConsumerLatencyMs(value);
  }

  public void recordProducerLocalBrokerLatencyMs(String storeName, int version, double value) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordProducerLocalBrokerLatencyMs(value);
    if(getStats(storeName, version) != null) getStats(storeName, version).recordProducerLocalBrokerLatencyMs(value);
  }

  public void recordLocalBrokerFollowerConsumerLatencyMs(String storeName, int version, double value) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordLocalBrokerFollowerConsumerLatencyMs(value);
    if(getStats(storeName, version) != null) getStats(storeName, version).recordLocalBrokerFollowerConsumerLatencyMs(value);
  }

  public void recordProducerFollowerConsumerLatencyMs(String storeName, int version, double value) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordProducerFollowerConsumerLatencyMs(value);
    if(getStats(storeName, version) != null) getStats(storeName, version).recordProducerFollowerConsumerLatencyMs(value);
  }

  public void recordBenignLeaderOffsetRewind(String storeName, int version) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordBenignLeaderOffsetRewind();
    if(getStats(storeName, version) != null) getStats(storeName, version).recordBenignLeaderOffsetRewind();
  }

  public void recordPotentiallyLossyLeaderOffsetRewind(String storeName, int version) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordPotentiallyLossyLeaderOffsetRewind();
    if(getStats(storeName, version) != null) getStats(storeName, version).recordPotentiallyLossyLeaderOffsetRewind();
  }

  public void recordLeaderProducerFailure(String storeName, int version) {
    if(getTotalStats(storeName) != null) getTotalStats(storeName).recordLeaderProducerFailure();
    if(getStats(storeName, version) != null) getStats(storeName, version).recordLeaderProducerFailure();
  }
}
