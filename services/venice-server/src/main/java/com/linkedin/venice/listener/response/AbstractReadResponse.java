package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import io.netty.buffer.ByteBuf;


/**
 * This class is used to store common fields shared by various read responses.
 */
public abstract class AbstractReadResponse implements ReadResponse {
  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
  private boolean isStreamingResponse = false;
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

  public boolean isFound() {
    return true;
  }

  public abstract ByteBuf getResponseBody();

  public abstract int getResponseSchemaIdHeader();

  public abstract ReadResponseStatsRecorder getStatsRecorder();
}
