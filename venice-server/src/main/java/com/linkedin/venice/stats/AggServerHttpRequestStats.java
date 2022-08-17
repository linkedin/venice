package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MetricsRepository;


public class AggServerHttpRequestStats extends AbstractVeniceAggStats<ServerHttpRequestStats> {
  public AggServerHttpRequestStats(MetricsRepository metricsRepository, RequestType requestType) {
    super(
        metricsRepository,
        (metricsRepo, storeName) -> new ServerHttpRequestStats(metricsRepo, storeName, requestType));
  }

  public AggServerHttpRequestStats(
      MetricsRepository metricsRepository,
      RequestType requestType,
      boolean isKeyValueProfilingEnabled) {
    super(
        metricsRepository,
        (
            metricsRepo,
            storeName) -> new ServerHttpRequestStats(metricsRepo, storeName, requestType, isKeyValueProfilingEnabled));
  }

  public void recordSuccessRequest(String storeName) {
    totalStats.recordSuccessRequest();
    getStoreStats(storeName).recordSuccessRequest();
  }

  public void recordErrorRequest() {
    totalStats.recordErrorRequest();
  }

  public void recordErrorRequest(String storeName) {
    totalStats.recordErrorRequest();
    getStoreStats(storeName).recordErrorRequest();
  }

  public void recordSuccessRequestLatency(String storeName, double latency) {
    totalStats.recordSuccessRequestLatency(latency);
    getStoreStats(storeName).recordSuccessRequestLatency(latency);
  }

  public void recordErrorRequestLatency(double latency) {
    totalStats.recordErrorRequestLatency(latency);
  }

  public void recordErrorRequestLatency(String storeName, double latency) {
    totalStats.recordErrorRequestLatency(latency);
    getStoreStats(storeName).recordErrorRequestLatency(latency);
  }

  public void recordDatabaseLookupLatency(String storeName, double latency, boolean assembledMultiChunkLargeValue) {
    totalStats.recordDatabaseLookupLatency(latency, assembledMultiChunkLargeValue);
    getStoreStats(storeName).recordDatabaseLookupLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordRequestKeyCount(String storeName, int keyNum) {
    totalStats.recordRequestKeyCount(keyNum);
    getStoreStats(storeName).recordRequestKeyCount(keyNum);
  }

  public void recordSuccessRequestKeyCount(String storeName, int keyNum) {
    totalStats.recordSuccessRequestKeyCount(keyNum);
    getStoreStats(storeName).recordSuccessRequestKeyCount(keyNum);
  }

  public void recordRequestSizeInBytes(String storeName, int requestSizeInBytes) {
    totalStats.recordRequestSizeInBytes(requestSizeInBytes);
    getStoreStats(storeName).recordRequestSizeInBytes(requestSizeInBytes);
  }

  public void recordMultiChunkLargeValueCount(String storeName, int multiChunkLargeValueCount) {
    totalStats.recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
    getStoreStats(storeName).recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
  }

  public void recordStorageExecutionHandlerSubmissionWaitTime(double submissionWaitTime) {
    totalStats.recordStorageExecutionHandlerSubmissionWaitTime(submissionWaitTime);
  }

  public void recordStorageExecutionQueueLen(int len) {
    totalStats.recordStorageExecutionQueueLen(len);
  }

  public void recordRequestFirstPartLatency(String storeName, double latency) {
    totalStats.recordRequestFirstPartLatency(latency);
    getStoreStats(storeName).recordRequestFirstPartLatency(latency);
  }

  public void recordRequestSecondPartLatency(String storeName, double latency) {
    totalStats.recordRequestSecondPartLatency(latency);
    getStoreStats(storeName).recordRequestSecondPartLatency(latency);
  }

  public void recordRequestPartsInvokeDelayLatency(String storeName, double latency) {
    totalStats.recordRequestPartsInvokeDelayLatency(latency);
    getStoreStats(storeName).recordRequestPartsInvokeDelayLatency(latency);
  }

  public void recordRequestPartCount(String storeName, int partCount) {
    totalStats.recordRequestPartCount(partCount);
    getStoreStats(storeName).recordRequestPartCount(partCount);
  }

  public void recordReadComputeLatency(String storeName, double latency, boolean assembledMultiChunkLargeValue) {
    totalStats.recordReadComputeLatency(latency, assembledMultiChunkLargeValue);
    getStoreStats(storeName).recordReadComputeLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordReadComputeDeserializationLatency(
      String storeName,
      double latency,
      boolean assembledMultiChunkLargeValue) {
    totalStats.recordReadComputeDeserializationLatency(latency, assembledMultiChunkLargeValue);
    getStoreStats(storeName).recordReadComputeDeserializationLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordReadComputeSerializationLatency(
      String storeName,
      double latency,
      boolean assembledMultiChunkLargeValue) {
    totalStats.recordReadComputeSerializationLatency(latency, assembledMultiChunkLargeValue);
    getStoreStats(storeName).recordReadComputeSerializationLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordDotProductCount(String storeName, int count) {
    totalStats.recordDotProductCount(count);
    getStoreStats(storeName).recordDotProductCount(count);
  }

  public void recordCosineSimilarityCount(String storeName, int count) {
    totalStats.recordCosineSimilarityCount(count);
    getStoreStats(storeName).recordCosineSimilarityCount(count);
  }

  public void recordHadamardProductCount(String storeName, int count) {
    totalStats.recordHadamardProduct(count);
    getStoreStats(storeName).recordHadamardProduct(count);
  }

  public void recordCountOperatorCount(String storeName, int count) {
    totalStats.recordCountOperator(count);
    getStoreStats(storeName).recordCountOperator(count);
  }

  public void recordEarlyTerminatedEarlyRequest(String storeName) {
    totalStats.recordEarlyTerminatedEarlyRequest();
    getStoreStats(storeName).recordEarlyTerminatedEarlyRequest();
  }

  public void recordKeySizeInByte(String storeName, long keySize) {
    totalStats.recordKeySizeInByte(keySize);
    getStoreStats(storeName).recordKeySizeInByte(keySize);
  }

  public void recordValueSizeInByte(String storeName, long valueSize) {
    totalStats.recordValueSizeInByte(valueSize);
    getStoreStats(storeName).recordValueSizeInByte(valueSize);
  }
}
