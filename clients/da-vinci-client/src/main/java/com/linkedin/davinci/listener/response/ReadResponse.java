package com.linkedin.davinci.listener.response;

import com.linkedin.venice.compression.CompressionStrategy;
import io.netty.buffer.ByteBuf;


/**
 * This is used to store common fields shared by various read responses. It is intended to store state required to
 * fulfill the service's goals from the perspective of external callers (e.g. the Router or Fast Client).
 *
 * See also {@link #getStats()} which returns the container used to store state exclusively used by metrics.
 */
public interface ReadResponse {
  ReadResponseStats getStats();

  void setCompressionStrategy(CompressionStrategy compressionStrategy);

  void setStreamingResponse();

  boolean isStreamingResponse();

  CompressionStrategy getCompressionStrategy();

  /**
   * Set the read compute unit (RCU) cost for this response's request
   * @param rcu
   */
  void setRCU(int rcu);

  /**
   * Get the read compute unit (RCU) for this response's request
   * @return
   */
  int getRCU();

  boolean isFound();

  ByteBuf getResponseBody();

  int getResponseSchemaIdHeader();
}
