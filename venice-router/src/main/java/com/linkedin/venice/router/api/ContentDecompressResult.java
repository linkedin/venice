package com.linkedin.venice.router.api;

import com.linkedin.venice.compression.CompressionStrategy;
import io.netty.buffer.ByteBuf;


/**
 * A POJO to store the response content and additional compression related metadata.
 */
public class ContentDecompressResult {
  private final ByteBuf content;
  private final CompressionStrategy compressionStrategy;
  private final long decompressionTimeInNs;

  public ContentDecompressResult(ByteBuf content, CompressionStrategy compressionStrategy, long decompressionTimeInNs) {
    this.content = content;
    this.compressionStrategy = compressionStrategy;
    this.decompressionTimeInNs = decompressionTimeInNs;
  }

  public ByteBuf getContent() {
    return content;
  }

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public long getDecompressionTimeInNs() {
    return decompressionTimeInNs;
  }
}
