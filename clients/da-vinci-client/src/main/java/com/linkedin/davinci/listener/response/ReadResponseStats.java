package com.linkedin.davinci.listener.response;

/**
 * This class is used to accumulate stats associated with a read response.
 *
 * This container is purely for metrics-related work, and should not be used to store any state which is functionally
 * required to achieve the service's goal. Contrast with {@link ReadResponse}, which wraps this one.
 *
 * The reason to keep them separate is that the state used for metrics has a slightly longer lifespan than that used for
 * functional purposes, since we record metrics at the very end, after the response is sent back. This allows us to free
 * up the functional state sooner, even while we need to hang on to the metrics state.
 */
public interface ReadResponseStats {
  void addDatabaseLookupLatency(double latency);

  void addReadComputeLatency(double latency);

  void addReadComputeDeserializationLatency(double latency);

  void addReadComputeSerializationLatency(double latency);

  void addKeySize(int size);

  void addValueSize(int size);

  void addReadComputeOutputSize(int size);

  void incrementDotProductCount(int count);

  void incrementCountOperatorCount(int count);

  void incrementCosineSimilarityCount(int count);

  void incrementHadamardProductCount(int count);

  void setStorageExecutionSubmissionWaitTime(double storageExecutionSubmissionWaitTime);

  void setStorageExecutionQueueLen(int storageExecutionQueueLen);

  void incrementMultiChunkLargeValueCount();
}
