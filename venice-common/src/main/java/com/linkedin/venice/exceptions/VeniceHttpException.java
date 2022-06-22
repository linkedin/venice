package com.linkedin.venice.exceptions;

public class VeniceHttpException extends VeniceException {
  private final int statusCode;

  public VeniceHttpException(int statusCode) {
    super();
    super.exceptionType = ExceptionType.CONNECTION_ERROR;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message) {
    super(message);
    super.exceptionType = ExceptionType.CONNECTION_ERROR;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message, ExceptionType type) {
    super(message);
    super.exceptionType = type;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, Throwable cause) {
    super(cause);
    super.exceptionType = ExceptionType.CONNECTION_ERROR;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, Throwable cause, ExceptionType type) {
    super(cause);
    super.exceptionType = type;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message, Throwable cause, ExceptionType exceptionType) {
    super(message, cause);
    super.exceptionType = exceptionType;
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message, Throwable cause) {
    super(message, cause);
    super.exceptionType = ExceptionType.CONNECTION_ERROR;
    this.statusCode = statusCode;
  }

  @Override
  public int getHttpStatusCode() {
    return this.statusCode;
  }

  @Override
  public String getMessage() {
    return "Http Status " + getHttpStatusCode() + " - " + super.getMessage();
  }
}
