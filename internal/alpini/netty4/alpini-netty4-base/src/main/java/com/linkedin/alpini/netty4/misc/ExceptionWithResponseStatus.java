package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.misc.ExceptionWithStatus;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ExceptionWithResponseStatus extends ExceptionWithStatus {
  public ExceptionWithResponseStatus(@Nonnull HttpResponseStatus responseStatus) {
    this(responseStatus, responseStatus.reasonPhrase());
  }

  public ExceptionWithResponseStatus(@Nonnull HttpResponseStatus responseStatus, @Nonnull String message) {
    super(HttpResponseStatus.class, responseStatus, responseStatus.code(), message);
  }

  public ExceptionWithResponseStatus(@Nonnull HttpResponseStatus responseStatus, Throwable cause) {
    this(responseStatus, responseStatus.reasonPhrase(), cause);
  }

  public ExceptionWithResponseStatus(
      @Nonnull HttpResponseStatus responseStatus,
      @Nonnull String message,
      Throwable cause) {
    super(HttpResponseStatus.class, responseStatus, responseStatus.code(), message, cause);
  }

  protected ExceptionWithResponseStatus(
      @Nonnull HttpResponseStatus responseStatus,
      Throwable cause,
      boolean enableSuppression,
      boolean writableStackTrace) {
    this(responseStatus, responseStatus.reasonPhrase(), cause, enableSuppression, writableStackTrace);
  }

  protected ExceptionWithResponseStatus(
      @Nonnull HttpResponseStatus responseStatus,
      @Nonnull String message,
      Throwable cause,
      boolean enableSuppression,
      boolean writableStackTrace) {
    super(
        HttpResponseStatus.class,
        responseStatus,
        responseStatus.code(),
        message,
        cause,
        enableSuppression,
        writableStackTrace);
  }

  public HttpResponseStatus status() {
    return status(HttpResponseStatus.class).get();
  }

  public HttpResponseStatus responseStatus() {
    return status();
  }
}
