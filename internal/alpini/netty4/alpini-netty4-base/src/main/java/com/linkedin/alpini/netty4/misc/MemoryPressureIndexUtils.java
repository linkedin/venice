package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.base.misc.MemoryPressureIndexMonitor;
import com.linkedin.alpini.base.misc.StringToNumberUtils;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.AttributeKey;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;


public enum MemoryPressureIndexUtils {
  SINGLETON;

  // Some get Request's having contentLength set to zero. Such request would not change the value of MPI.
  // So we added a MINIMUM_BYTES_TO_ADD for them. Given that MPI is a guessing instead of an accurate calculation of
  // memory allocation, giving a consistent number to all simple requests that don't have body should be fine.
  private static final int MINIMUM_BYTES_TO_ADD = 32;

  public static final String X_ESPRESSO_REQUEST_ID = "X-ESPRESSO-Request-Id";

  private static final AttributeKey<String> REQUEST_ID =
      AttributeKey.valueOf(MemoryPressureIndexMonitor.class, "RequestID");

  private static final Function<HttpRequest, Optional<String>> DEFAULT_REQUEST_TO_KEY_FUNCTION =
      r -> Optional.ofNullable(r.headers().get(X_ESPRESSO_REQUEST_ID));

  private static final Function<HttpResponse, Optional<String>> DEFAULT_RESPONSE_TO_KEY_FUNCTION =
      r -> Optional.ofNullable(r.headers().get(X_ESPRESSO_REQUEST_ID));

  public static Function<HttpRequest, Optional<String>> defaultRequestToKeyFunction() {
    return DEFAULT_REQUEST_TO_KEY_FUNCTION;
  }

  public static Function<HttpResponse, Optional<String>> defaultResponseToKeyFunction() {
    return DEFAULT_RESPONSE_TO_KEY_FUNCTION;
  }

  public static AttributeKey<String> requestId() {
    return REQUEST_ID;
  }

  /**
   * Try the best to get ContentLength
   * 1. First it is simply get the length from the headers. If the contentLength is 0, return a fixed length
   * @param m Http message
   * @return Content length
   */
  public static Optional<Integer> getContentLength(HttpMessage m) {
    Objects.requireNonNull(m, "HttpMessage");
    return StringToNumberUtils.fromString(m.headers().get(HeaderNames.CONTENT_LENGTH)).map(length -> {
      if (length <= 0 && m instanceof FullHttpMessage && ((FullHttpMessage) m).content() != null) {
        length = ((FullHttpMessage) m).content().readableBytes();
      }
      return length <= 0 ? MINIMUM_BYTES_TO_ADD : length;
    });
  }

  public static int getMinimumBytesToAdd() {
    return MINIMUM_BYTES_TO_ADD;
  }
}
