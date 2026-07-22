package com.linkedin.venice.exceptions;

public class VeniceBlobTransferHttpException extends VeniceException {
  private final int statusCode;

  public VeniceBlobTransferHttpException(int statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
