package com.linkedin.venice.acl;

public class AclException extends Exception {
  public AclException() {
    super();
  }

  public AclException(String message) {
    super(message);
  }

  public AclException(String message, Throwable cause) {
    super(message, cause);
  }

  public AclException(Throwable cause) {
    super(cause);
  }

  protected AclException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
