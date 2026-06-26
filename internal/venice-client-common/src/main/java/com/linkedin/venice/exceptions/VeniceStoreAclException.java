package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


/**
 * Customized exception for ACL related errors in Venice store operations. A dedicated exception helps to propagate
 * the ACL error information to the VPJ layer where it categorizes the error as user related.
 */
public class VeniceStoreAclException extends VeniceException {
  public VeniceStoreAclException(String message) {
    super(message);
    super.errorType = ErrorType.ACL_ERROR;
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_FORBIDDEN;
  }
}
