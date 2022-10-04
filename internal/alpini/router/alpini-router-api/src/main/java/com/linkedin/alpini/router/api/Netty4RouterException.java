package com.linkedin.alpini.router.api;

import io.netty.handler.codec.http.HttpResponseStatus;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class Netty4RouterException extends RouterException {
  public Netty4RouterException(@Nonnull HttpResponseStatus status, boolean closeChannel) {
    this(status, status.reasonPhrase(), closeChannel);
  }

  public Netty4RouterException(@Nonnull HttpResponseStatus status, @Nonnull String message, boolean closeChannel) {
    super(HttpResponseStatus.class, status, status.code(), message, closeChannel);
  }

  public Netty4RouterException(@Nonnull HttpResponseStatus status, boolean closeChannel, Throwable cause) {
    this(status, status.reasonPhrase(), closeChannel, cause);
  }

  public Netty4RouterException(
      @Nonnull HttpResponseStatus status,
      @Nonnull String message,
      boolean closeChannel,
      Throwable cause) {
    super(HttpResponseStatus.class, status, status.code(), message, closeChannel, cause);
  }

  public Netty4RouterException(
      @Nonnull HttpResponseStatus status,
      boolean closeChannel,
      Throwable cause,
      boolean enableSuppression,
      boolean writableStackTrace) {
    this(status, status.reasonPhrase(), closeChannel, cause, enableSuppression, writableStackTrace);
  }

  public Netty4RouterException(
      @Nonnull HttpResponseStatus status,
      @Nonnull String message,
      boolean closeChannel,
      Throwable cause,
      boolean enableSuppression,
      boolean writableStackTrace) {
    super(
        HttpResponseStatus.class,
        status,
        status.code(),
        message,
        closeChannel,
        cause,
        enableSuppression,
        writableStackTrace);
  }

  public HttpResponseStatus status() {
    return status(HttpResponseStatus.class).get();
  }
}
