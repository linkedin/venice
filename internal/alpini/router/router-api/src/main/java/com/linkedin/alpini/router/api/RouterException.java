package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.misc.ExceptionWithStatus;
import javax.annotation.Nonnull;


/**
 * Exception class for checked exceptions within the Espresso Router. This extends Exception with
 * the addition of a ResponseStatus for the error.
 *
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 */
public class RouterException extends ExceptionWithStatus {
  private static final long serialVersionUID = 1L;

  private final boolean _closeChannel;

  public <STATUS> RouterException(
      @Nonnull Class<STATUS> statusClass,
      @Nonnull STATUS status,
      int code,
      @Nonnull String message,
      boolean closeChannel) {
    super(statusClass, status, code, message);
    _closeChannel = closeChannel;
  }

  public <STATUS> RouterException(
      @Nonnull Class<STATUS> statusClass,
      @Nonnull STATUS status,
      int code,
      @Nonnull String message,
      boolean closeChannel,
      Throwable cause) {
    super(statusClass, status, code, message, cause);
    _closeChannel = closeChannel;
  }

  public <STATUS> RouterException(
      @Nonnull Class<STATUS> statusClass,
      @Nonnull STATUS status,
      int code,
      @Nonnull String message,
      boolean closeChannel,
      Throwable cause,
      boolean enableSuppression,
      boolean writableStackTrace) {
    super(statusClass, status, code, message, cause, enableSuppression, writableStackTrace);
    _closeChannel = closeChannel;
  }

  public boolean shouldCloseChannel() {
    return _closeChannel;
  }
}
