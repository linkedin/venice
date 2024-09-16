package com.linkedin.venice.listener.response;

import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;


public class BinaryResponse {
  private final ByteBuf body;
  private final VeniceReadResponseStatus status;

  public BinaryResponse(ByteBuf body) {
    if (body == null) {
      this.body = Unpooled.EMPTY_BUFFER;
      this.status = VeniceReadResponseStatus.KEY_NOT_FOUND;
    } else {
      this.body = body;
      this.status = VeniceReadResponseStatus.OK;
    }
  }

  public BinaryResponse(ByteBuffer body) {
    this(body == null ? null : Unpooled.wrappedBuffer(body));
  }

  public ByteBuf getBody() {
    return body;
  }

  public VeniceReadResponseStatus getStatus() {
    return status;
  }
}
