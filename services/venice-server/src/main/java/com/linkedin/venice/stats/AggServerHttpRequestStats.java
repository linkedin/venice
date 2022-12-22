package com.linkedin.venice.stats;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MetricsRepository;


/**
 * {@code AggServerHttpRequestStats} is the aggregate statistics for {@code ServerHttpRequestStats} corresponding to
 * the type of requests defined in {@link RequestType}.
 */
public class AggServerHttpRequestStats extends AbstractVeniceAggStoreStats<ServerHttpRequestStats> {
  private ServerHttpRequestStats serverHttpRequestStats;

  public AggServerHttpRequestStats(
      MetricsRepository metricsRepository,
      RequestType requestType,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled) {
    super(
        metricsRepository,
        (metricsRepo, storeName) -> new ServerHttpRequestStats(metricsRepo, storeName, requestType),
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled);
  }

  public AggServerHttpRequestStats(
      MetricsRepository metricsRepository,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled,
      ReadOnlyStoreRepository metadataRepository,
      boolean unregisterMetricForDeletedStoreEnabled) {
    super(
        metricsRepository,
        (
            metricsRepo,
            storeName) -> new ServerHttpRequestStats(metricsRepo, storeName, requestType, isKeyValueProfilingEnabled),
        metadataRepository,
        unregisterMetricForDeletedStoreEnabled);
  }

  public void setStoreStat(String storeName) {
    this.serverHttpRequestStats = super.getStoreStats(storeName);
  }

  ServerHttpRequestStats getStoreStat(String storeName) {
    if (serverHttpRequestStats == null || !storeName.equals(serverHttpRequestStats.getStoreName())) {
      this.serverHttpRequestStats = super.getStoreStats(storeName);
    }
    return serverHttpRequestStats;
  }

  public void recordSuccessRequest(String storeName) {
    totalStats.recordSuccessRequest();
    getStoreStat(storeName).recordSuccessRequest();
  }

  public void recordErrorRequest() {
    totalStats.recordErrorRequest();
  }

  public void recordErrorRequest(String storeName) {
    totalStats.recordErrorRequest();
    getStoreStat(storeName).recordErrorRequest();
  }

  public void recordSuccessRequestLatency(String storeName, double latency) {
    totalStats.recordSuccessRequestLatency(latency);
    getStoreStat(storeName).recordSuccessRequestLatency(latency);
  }

  public void recordErrorRequestLatency(double latency) {
    totalStats.recordErrorRequestLatency(latency);
  }

  public void recordErrorRequestLatency(String storeName, double latency) {
    totalStats.recordErrorRequestLatency(latency);
    getStoreStat(storeName).recordErrorRequestLatency(latency);
  }

  public void recordDatabaseLookupLatency(String storeName, double latency, boolean assembledMultiChunkLargeValue) {
    totalStats.recordDatabaseLookupLatency(latency, assembledMultiChunkLargeValue);
    getStoreStat(storeName).recordDatabaseLookupLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordRequestKeyCount(String storeName, int keyNum) {
    totalStats.recordRequestKeyCount(keyNum);
    getStoreStat(storeName).recordRequestKeyCount(keyNum);
  }

  public void recordSuccessRequestKeyCount(String storeName, int keyNum) {
    totalStats.recordSuccessRequestKeyCount(keyNum);
    getStoreStat(storeName).recordSuccessRequestKeyCount(keyNum);
  }

  public void recordRequestSizeInBytes(String storeName, int requestSizeInBytes) {
    totalStats.recordRequestSizeInBytes(requestSizeInBytes);
    getStoreStat(storeName).recordRequestSizeInBytes(requestSizeInBytes);
  }

  public void recordMultiChunkLargeValueCount(String storeName, int multiChunkLargeValueCount) {
    totalStats.recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
    getStoreStat(storeName).recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
  }

  public void recordStorageExecutionHandlerSubmissionWaitTime(double submissionWaitTime) {
    totalStats.recordStorageExecutionHandlerSubmissionWaitTime(submissionWaitTime);
  }

  public void recordStorageExecutionQueueLen(int len) {
    totalStats.recordStorageExecutionQueueLen(len);
  }

  public void recordRequestFirstPartLatency(String storeName, double latency) {
    totalStats.recordRequestFirstPartLatency(latency);
    getStoreStat(storeName).recordRequestFirstPartLatency(latency);
  }

  public void recordRequestSecondPartLatency(String storeName, double latency) {
    totalStats.recordRequestSecondPartLatency(latency);
    getStoreStat(storeName).recordRequestSecondPartLatency(latency);
  }

  public void recordRequestPartsInvokeDelayLatency(String storeName, double latency) {
    totalStats.recordRequestPartsInvokeDelayLatency(latency);
    getStoreStat(storeName).recordRequestPartsInvokeDelayLatency(latency);
  }

  public void recordRequestPartCount(String storeName, int partCount) {
    totalStats.recordRequestPartCount(partCount);
    getStoreStat(storeName).recordRequestPartCount(partCount);
  }

  public void recordReadComputeLatency(String storeName, double latency, boolean assembledMultiChunkLargeValue) {
    totalStats.recordReadComputeLatency(latency, assembledMultiChunkLargeValue);
    getStoreStat(storeName).recordReadComputeLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordReadComputeDeserializationLatency(
      String storeName,
      double latency,
      boolean assembledMultiChunkLargeValue) {
    totalStats.recordReadComputeDeserializationLatency(latency, assembledMultiChunkLargeValue);
    getStoreStat(storeName).recordReadComputeDeserializationLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordReadComputeSerializationLatency(
      String storeName,
      double latency,
      boolean assembledMultiChunkLargeValue) {
    totalStats.recordReadComputeSerializationLatency(latency, assembledMultiChunkLargeValue);
    getStoreStat(storeName).recordReadComputeSerializationLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordDotProductCount(String storeName, int count) {
    totalStats.recordDotProductCount(count);
    getStoreStat(storeName).recordDotProductCount(count);
  }

  public void recordCosineSimilarityCount(String storeName, int count) {
    totalStats.recordCosineSimilarityCount(count);
    getStoreStat(storeName).recordCosineSimilarityCount(count);
  }

  public void recordHadamardProductCount(String storeName, int count) {
    totalStats.recordHadamardProduct(count);
    getStoreStat(storeName).recordHadamardProduct(count);
  }

  public void recordCountOperatorCount(String storeName, int count) {
    totalStats.recordCountOperator(count);
    getStoreStat(storeName).recordCountOperator(count);
  }

  public void recordEarlyTerminatedEarlyRequest(String storeName) {
    totalStats.recordEarlyTerminatedEarlyRequest();
    getStoreStat(storeName).recordEarlyTerminatedEarlyRequest();
  }

  public void recordKeySizeInByte(String storeName, long keySize) {
    totalStats.recordKeySizeInByte(keySize);
    getStoreStat(storeName).recordKeySizeInByte(keySize);
  }

  public void recordValueSizeInByte(String storeName, long valueSize) {
    totalStats.recordValueSizeInByte(valueSize);
    getStoreStat(storeName).recordValueSizeInByte(valueSize);
  }
}
