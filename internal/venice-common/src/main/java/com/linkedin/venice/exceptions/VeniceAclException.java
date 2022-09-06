package com.linkedin.venice.exceptions;

/**
 * A runtime exception which may be thrown by implementation of AuthorizerService interface.
 */
public class VeniceAclException extends VeniceException {
  int httpErrorCode;

  public VeniceAclException(String message, int httpErrorCode) {
    super(message);
    this.httpErrorCode = httpErrorCode;
  }

  public VeniceAclException(String message, int httpErrorCode, Throwable throwable) {
    super(message, throwable);
    this.httpErrorCode = httpErrorCode;
  }

  public String getMessage() {
    return "AclException ErrorCode: " + getHttpStatusCode() + " - " + super.getMessage();
  }

  @Override
  public int getHttpStatusCode() {
    return httpErrorCode;
  }

}
