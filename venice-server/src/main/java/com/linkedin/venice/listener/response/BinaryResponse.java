package com.linkedin.venice.listener.response;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.ByteBuffer;


public class BinaryResponse {
  private final ByteBuf body;
  private final HttpResponseStatus status;

  public BinaryResponse(ByteBuf body) {
    if (body == null) {
      this.body = Unpooled.EMPTY_BUFFER;
      this.status = HttpResponseStatus.NOT_FOUND;
    } else {
      this.body = body;
      this.status = HttpResponseStatus.OK;
    }
  }

  public BinaryResponse(ByteBuffer body) {
    this(body == null ? null : Unpooled.wrappedBuffer(body));
  }

  public ByteBuf getBody() {
    return body;
  }

  public HttpResponseStatus getStatus() {
    return status;
  }
}
