package com.linkedin.venice.listener.response;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * This class is used to store common fields shared by various read responses.
 */
public abstract class ReadResponse {
  private double bdbQueryLatency;
  private int multiChunkLargeValueCount = 0;

  public void setBdbQueryLatency(double latency) {
    this.bdbQueryLatency = latency;
  }

  public double getBdbQueryLatency() {
    return this.bdbQueryLatency;
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
