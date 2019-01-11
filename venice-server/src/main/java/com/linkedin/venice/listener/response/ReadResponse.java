package com.linkedin.venice.listener.response;

import com.linkedin.venice.compression.CompressionStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * This class is used to store common fields shared by various read responses.
 */
public abstract class ReadResponse {
  private double bdbQueryLatency = -1;
  private double readComputeLatency = -1;
  private double readComputeDeserializationLatency = -1;
  private double readComputeSerializationLatency = -1;
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

  public double getReadComputeDeserializationLatency() {
    return this.readComputeDeserializationLatency;
  }

  public void setReadComputeSerializationLatency(double latency) {
    this.readComputeSerializationLatency = latency;
  }

  public void addReadComputeSerializationLatency(double latency) {
    this.readComputeSerializationLatency += latency;
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
