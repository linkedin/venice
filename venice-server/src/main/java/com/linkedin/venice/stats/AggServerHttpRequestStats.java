package com.linkedin.venice.stats;

import com.linkedin.venice.read.RequestType;
import io.tehuti.metrics.MetricsRepository;


public class AggServerHttpRequestStats extends AbstractVeniceAggStats<ServerHttpRequestStats> {
  public AggServerHttpRequestStats(MetricsRepository metricsRepository, RequestType requestType) {
    super(metricsRepository,
          (metricsRepo, storeName) -> new ServerHttpRequestStats(metricsRepo, storeName, requestType));
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

  public void recordErrorRequestLatency (double latency) {
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

  public void recordMultiChunkLargeValueCount(String storeName, int multiChunkLargeValueCount) {
    totalStats.recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
    getStoreStats(storeName).recordMultiChunkLargeValueCount(multiChunkLargeValueCount);
  }

  public void recordStorageExecutionHandlerSubmissionWaitTime(double submissionWaitTime) {
    totalStats.recordStorageExecutionHandlerSubmissionWaitTime(submissionWaitTime);
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

  public void recordReadComputeDeserializationLatency(String storeName, double latency, boolean assembledMultiChunkLargeValue) {
    totalStats.recordReadComputeDeserializationLatency(latency, assembledMultiChunkLargeValue);
    getStoreStats(storeName).recordReadComputeDeserializationLatency(latency, assembledMultiChunkLargeValue);
  }

  public void recordReadComputeSerializationLatency(String storeName, double latency, boolean assembledMultiChunkLargeValue) {
    totalStats.recordReadComputeSerializationLatency(latency, assembledMultiChunkLargeValue);
    getStoreStats(storeName).recordReadComputeSerializationLatency(latency, assembledMultiChunkLargeValue);
  }
}
