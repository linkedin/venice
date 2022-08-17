package com.linkedin.venice.streaming;

import com.linkedin.venice.HttpConstants;
import io.netty.handler.codec.http.HttpRequest;
import java.nio.ByteBuffer;


public class StreamingUtils {
  public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

  public static boolean isStreamingEnabled(HttpRequest request) {
    return request.headers().contains(HttpConstants.VENICE_STREAMING);
  }
}
