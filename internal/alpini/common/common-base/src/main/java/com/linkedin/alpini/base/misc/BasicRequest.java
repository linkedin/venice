package com.linkedin.alpini.base.misc;

import java.util.UUID;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface BasicRequest {
  /**
   * The request method name
   * @return method string
   */
  String getMethodName();

  /**
   * The request URI
   * @return uri string
   */
  String getUri();

  /**
   * A computed UUID for the request
   * @return uuid
   */
  UUID getRequestId();

  /**
   * Timestamp at which the request was received.
   * @return timestamp
   * @see Time#currentTimeMillis()
   */
  long getRequestTimestamp();

  /**
   * High precision nanotime at which the request was received.
   * @return timestamp
   * @see Time#nanoTime()
   */
  long getRequestNanos();

  /**
   * The content length of the request body in bytes, as retrieved by the header,
   * or -1 if not known.
   * @return content length
   */
  long getRequestContentLength();

  /**
   * The request headers.
   * @return headers
   */
  Headers getRequestHeaders();

  static long getRequestTimestamp(Object o) {
    if (o instanceof BasicRequest) {
      return ((BasicRequest) o).getRequestTimestamp();
    } else {
      return Time.currentTimeMillis();
    }
  }
}
