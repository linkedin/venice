package com.linkedin.alpini.base.misc;

import java.util.Optional;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ExceptionWithStatus extends Exception {
  private final Class<?> _statusClass;
  private final Object _status;
  private final int _code;

  public <STATUS> ExceptionWithStatus(
      @Nonnull Class<STATUS> statusClass,
      @Nonnull STATUS status,
      int code,
      @Nonnull String message) {
    super(message);
    _statusClass = statusClass;
    _status = status;
    _code = code;
  }

  public <STATUS> ExceptionWithStatus(
      @Nonnull Class<STATUS> statusClass,
      @Nonnull STATUS status,
      int code,
      @Nonnull String message,
      Throwable cause) {
    super(message, cause);
    _statusClass = statusClass;
    _status = status;
    _code = code;
  }

  protected <STATUS> ExceptionWithStatus(
      @Nonnull Class<STATUS> statusClass,
      @Nonnull STATUS status,
      int code,
      @Nonnull String message,
      Throwable cause,
      boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
    _statusClass = statusClass;
    _status = status;
    _code = code;
  }

  public <STATUS> Optional<STATUS> status(Class<STATUS> statusClass) {
    return _statusClass.equals(statusClass) ? Optional.of(statusClass.cast(_status)) : Optional.empty();
  }

  public Object status() {
    return _status;
  }

  public int code() {
    return _code;
  }
}
