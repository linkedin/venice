package com.linkedin.davinci.listener.response;

import com.linkedin.venice.compression.CompressionStrategy;
import io.netty.buffer.ByteBuf;
import java.util.List;


/**
 * This class is used to store common fields shared by various read responses.
 */
public abstract class ReadResponse {
  private double databaseLookupLatency = -1;
  private double readComputeLatency = -1;
  private double readComputeDeserializationLatency = -1;
  private double readComputeSerializationLatency = -1;
  private double storageExecutionSubmissionWaitTime;
  private int storageExecutionQueueLen = -1;
  private int multiChunkLargeValueCount = 0;
  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
  private boolean isStreamingResponse = false;
  private List<Integer> keyListSize;
  private List<Integer> valueListSize;
  private int dotProductCount = 0;
  private int cosineSimilarityCount = 0;
  private int hadamardProductCount = 0;
  private int countOperatorCount = 0;
  private int rcu = 0;

  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public void setStreamingResponse() {
    this.isStreamingResponse = true;
  }

  public boolean isStreamingResponse() {
    return this.isStreamingResponse;
  }

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public void setDatabaseLookupLatency(double latency) {
    this.databaseLookupLatency = latency;
  }

  public void addDatabaseLookupLatency(double latency) {
    this.databaseLookupLatency += latency;
  }

  public double getDatabaseLookupLatency() {
    return this.databaseLookupLatency;
  }

  public void setReadComputeLatency(double latency) {
    this.readComputeLatency = latency;
  }

  public void addReadComputeLatency(double latency) {
    this.readComputeLatency += latency;
  }

  public double getReadComputeLatency() {
    return this.readComputeLatency;
  }

  public void setReadComputeDeserializationLatency(double latency) {
    this.readComputeDeserializationLatency = latency;
  }

  public void addReadComputeDeserializationLatency(double latency) {
    this.readComputeDeserializationLatency += latency;
  }

  public void setKeyListSize(List<Integer> keyListSize) {
    this.keyListSize = keyListSize;
  }

  public void setValueListSize(List<Integer> valueListSize) {
    this.valueListSize = valueListSize;
  }

  public double getReadComputeDeserializationLatency() {
    return this.readComputeDeserializationLatency;
  }

  public void setReadComputeSerializationLatency(double latency) {
    this.readComputeSerializationLatency = latency;
  }

  public void addReadComputeSerializationLatency(double latency) {
    this.readComputeSerializationLatency += latency;
  }

  public void incrementDotProductCount() {
    dotProductCount++;
  }

  public void incrementCountOperatorCount() {
    countOperatorCount++;
  }

  public void incrementCosineSimilarityCount() {
    cosineSimilarityCount++;
  }

  public void incrementHadamardProductCount() {
    hadamardProductCount++;
  }

  public double getReadComputeSerializationLatency() {
    return this.readComputeSerializationLatency;
  }

  public double getStorageExecutionHandlerSubmissionWaitTime() {
    return storageExecutionSubmissionWaitTime;
  }

  public void setStorageExecutionSubmissionWaitTime(double storageExecutionSubmissionWaitTime) {
    this.storageExecutionSubmissionWaitTime = storageExecutionSubmissionWaitTime;
  }

  /**
   * Set the read compute unit (RCU) cost for this response's request
   * @param rcu
   */
  public void setRCU(int rcu) {
    this.rcu = rcu;
  }

  /**
   * Get the read compute unit (RCU) for this response's request
   * @return
   */
  public int getRCU() {
    return this.rcu;
  }

  public int getStorageExecutionQueueLen() {
    return storageExecutionQueueLen;
  }

  public void setStorageExecutionQueueLen(int storageExecutionQueueLen) {
    this.storageExecutionQueueLen = storageExecutionQueueLen;
  }

  public void incrementMultiChunkLargeValueCount() {
    multiChunkLargeValueCount++;
  }

  public int getMultiChunkLargeValueCount() {
    return multiChunkLargeValueCount;
  }

  public boolean isFound() {
    return true;
  }

  public List<Integer> getKeySizeList() {
    return keyListSize;
  }

  public List<Integer> getValueSizeList() {
    return valueListSize;
  }

  public int getDotProductCount() {
    return dotProductCount;
  }

  public int getCosineSimilarityCount() {
    return cosineSimilarityCount;
  }

  public int getHadamardProductCount() {
    return hadamardProductCount;
  }

  public int getCountOperatorCount() {
    return countOperatorCount;
  }

  public abstract int getRecordCount();

  public abstract ByteBuf getResponseBody();

  public abstract int getResponseSchemaIdHeader();
}
