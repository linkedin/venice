package com.linkedin.alpini.base.misc;

/**
 * Client-facing exception for HTTP/2 too many active streams exception with a clearer message for the clients.
 *
 * ScatterGather handler somehow cannot find Netty's Http2Exception when testing.
 *
 * @author Yaoming Zhan <yzhan@linkedin.com>
 */
public class Http2TooManyStreamsException extends Exception {
  public static final String DEFAULT_MESSAGE = "Max concurrent requests limit reached for storage node connection";
  public static final Http2TooManyStreamsException INSTANCE = new Http2TooManyStreamsException();

  private Http2TooManyStreamsException() {
    super(DEFAULT_MESSAGE);
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    // No need to fill in the stack trace here, as that operation is expensive
    return this;
  }
}
