package com.linkedin.davinci.listener.response;

public class NoOpReadResponseStats implements ReadResponseStats {
  public static final NoOpReadResponseStats SINGLETON = new NoOpReadResponseStats();

  private NoOpReadResponseStats() {
  }

  @Override
  public long getCurrentTimeInNanos() {
    return 0;
  }

  @Override
  public void addDatabaseLookupLatency(long startTimeInNanos) {

  }

  @Override
  public void addReadComputeLatency(double latency) {

  }

  @Override
  public void addReadComputeDeserializationLatency(double latency) {

  }

  @Override
  public void addReadComputeSerializationLatency(double latency) {

  }

  @Override
  public void addKeySize(int size) {

  }

  @Override
  public void addValueSize(int size) {

  }

  @Override
  public void addReadComputeOutputSize(int size) {

  }

  @Override
  public void incrementDotProductCount(int count) {

  }

  @Override
  public void incrementCountOperatorCount(int count) {

  }

  @Override
  public void incrementCosineSimilarityCount(int count) {

  }

  @Override
  public void incrementHadamardProductCount(int count) {

  }

  @Override
  public void setStorageExecutionSubmissionWaitTime(double storageExecutionSubmissionWaitTime) {

  }

  @Override
  public void setStorageExecutionQueueLen(int storageExecutionQueueLen) {

  }

  @Override
  public void incrementMultiChunkLargeValueCount() {

  }
}
