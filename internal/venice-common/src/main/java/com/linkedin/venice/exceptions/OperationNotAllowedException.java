package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class OperationNotAllowedException extends VeniceHttpException {
  public OperationNotAllowedException(String message) {
    super(HttpStatus.SC_METHOD_NOT_ALLOWED, message);
  }
}
