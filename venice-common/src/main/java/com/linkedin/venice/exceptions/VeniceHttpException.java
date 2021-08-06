package com.linkedin.venice.exceptions;

public class VeniceHttpException extends VeniceException {
  private final int statusCode;
  protected ExceptionType exceptionType = ExceptionType.CONNECTION_ERROR;


  public VeniceHttpException(int statusCode) {
    super();
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, Throwable cause) {
    super(cause);
    this.statusCode = statusCode;
  }

  public VeniceHttpException(int statusCode, String message, Throwable cause) {
    super(message, cause);
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
