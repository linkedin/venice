package com.linkedin.alpini.netty4.misc;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectDecoder;
import io.netty.handler.codec.http.HttpVersion;


public class BasicHttpRequestDecoder extends HttpObjectDecoder {
  /**
   * Creates a new instance with the default
   * {@code maxInitialLineLength (4096)}, {@code maxHeaderSize (8192)}, and
   * {@code maxChunkSize (8192)}.
   */
  public BasicHttpRequestDecoder() {
  }

  /**
   * Creates a new instance with the specified parameters.
   */
  public BasicHttpRequestDecoder(int maxInitialLineLength, int maxHeaderSize, int maxChunkSize) {
    super(maxInitialLineLength, maxHeaderSize, maxChunkSize, true);
  }

  public BasicHttpRequestDecoder(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean validateHeaders) {
    super(maxInitialLineLength, maxHeaderSize, maxChunkSize, true, validateHeaders);
  }

  public BasicHttpRequestDecoder(
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean validateHeaders,
      int initialBufferSize) {
    super(maxInitialLineLength, maxHeaderSize, maxChunkSize, true, validateHeaders, initialBufferSize);
  }

  @Override
  protected HttpMessage createMessage(String[] initialLine) throws Exception {
    return new DefaultHttpRequest(
        HttpVersion.valueOf(initialLine[2]),
        HttpMethod.valueOf(initialLine[0]),
        initialLine[1],
        validateHeaders);
  }

  @Override
  protected HttpMessage createInvalidMessage() {
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/bad-request", validateHeaders);
  }

  @Override
  protected boolean isDecodingRequest() {
    return true;
  }
}
