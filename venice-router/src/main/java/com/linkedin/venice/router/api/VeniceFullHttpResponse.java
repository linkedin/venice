package com.linkedin.venice.router.api;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;


/**
 * A specialized {@link DefaultFullHttpResponse} object to record the decompression time of the records in the response.
 */
public class VeniceFullHttpResponse extends DefaultFullHttpResponse {
  private final long decompressionTimeInNs;

  public VeniceFullHttpResponse(
      HttpVersion version,
      HttpResponseStatus status,
      ByteBuf content,
      long decompressionTimeInNs) {
    super(version, status, content);
    this.decompressionTimeInNs = decompressionTimeInNs;
  }

  public long getDecompressionTimeInNs() {
    return decompressionTimeInNs;
  }
}
