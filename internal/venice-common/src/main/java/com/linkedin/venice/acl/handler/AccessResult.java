package com.linkedin.venice.acl.handler;

public enum AccessResult {
  GRANTED(),
  FORBIDDEN(
      "Access denied!\n"
          + "If you are the store owner, add this application (or your own username for Venice shell client) to the store ACL.\n"
          + "Otherwise, ask the store owner for read permission."
  ),
  UNAUTHORIZED(
      "ACL not found!\n" + "Either it has not been created, or can not be loaded.\n"
          + "Please create the ACL, or report the error if you know for sure that ACL exists for the store"
  ), ERROR_FORBIDDEN("Internal error occurred while checking ACL.\n" + "Please report this error to the Venice team.");

  private final String message;

  AccessResult() {
    this.message = null;
  }

  AccessResult(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
