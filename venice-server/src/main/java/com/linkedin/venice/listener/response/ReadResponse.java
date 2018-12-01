package com.linkedin.venice.listener.response;

import com.linkedin.venice.compression.CompressionStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * This class is used to store common fields shared by various read responses.
 */
public abstract class ReadResponse {
  private double bdbQueryLatency = -1;
  private double computeLatency = -1;
  private double deserializeLatency = -1;
  private double serializeLatency = -1;
  private double storageExecutionSubmissionWaitTime;
  private int multiChunkLargeValueCount = 0;
  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public void setBdbQueryLatency(double latency) {
    this.bdbQueryLatency = latency;
  }

  public double getBdbQueryLatency() {
    return this.bdbQueryLatency;
  }

  public void setComputeLatency(double latency) {
    this.computeLatency = latency;
  }

  public double getComputeLatency() {
    return this.computeLatency;
  }

  public void setDeserializeLatency(double latency) {
    this.deserializeLatency = latency;
  }

  public void addDeserializeLatency(double latency) {
    this.deserializeLatency += latency;
  }

  public double getDeserializeLatency() {
    return this.deserializeLatency;
  }

  public void setSerializeLatency(double latency) {
    this.serializeLatency = latency;
  }

  public void addSerializeLatency(double latency) {
    this.serializeLatency += latency;
  }

  public double getSerializeLatency() {
    return this.serializeLatency;
  }

  public double getStorageExecutionHandlerSubmissionWaitTime() {
    return storageExecutionSubmissionWaitTime;
  }

  public void setStorageExecutionSubmissionWaitTime(double storageExecutionSubmissionWaitTime) {
    this.storageExecutionSubmissionWaitTime = storageExecutionSubmissionWaitTime;
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

  public abstract int getRecordCount();

  public abstract ByteBuf getResponseBody();

  public abstract int getResponseSchemaIdHeader();

  public abstract String getResponseOffsetHeader();
}
