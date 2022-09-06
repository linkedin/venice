package com.linkedin.venice.router.streaming;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;


/**
 * Full response to indicate that all the sub responses are with good status: {@link HttpResponseStatus#OK}
 * for streaming request.
 */
public class SuccessfulStreamingResponse extends DefaultFullHttpResponse {
  public SuccessfulStreamingResponse() {
    super(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
  }
}
