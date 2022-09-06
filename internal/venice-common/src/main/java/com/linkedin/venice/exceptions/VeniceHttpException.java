package com.linkedin.venice.exceptions;

public class VeniceHttpException extends VeniceException {
  private final int statusCode;

  public VeniceHttpException(int statusCode) {
    super();
    super.errorType = ErrorType.CONNECTION_ERROR;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message) {
    super(message);
    super.errorType = ErrorType.CONNECTION_ERROR;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message, ErrorType type) {
    super(message);
    super.errorType = type;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, Throwable cause) {
    super(cause);
    super.errorType = ErrorType.CONNECTION_ERROR;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, Throwable cause, ErrorType type) {
    super(cause);
    super.errorType = type;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message, Throwable cause, ErrorType type) {
    super(message, cause);
    super.errorType = type;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message, Throwable cause) {
    super(message, cause);
    super.errorType = ErrorType.CONNECTION_ERROR;
    this.statusCode = statusCode;
  }

  @Override
  public int getHttpStatusCode() {
    return this.statusCode;
  }

  @Override
  public String getMessage() {
    return new StringBuilder().append("Http Status ")
        .append(getHttpStatusCode())
        .append(" - ")
        .append(super.getMessage())
        .toString();
  }
}
