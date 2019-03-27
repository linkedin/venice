package com.linkedin.venice.exceptions;

public class ZkDataAccessException extends VeniceException {
  public ZkDataAccessException(String path, String operation, int retryCount) {
    super("Can not do operation:" + operation + " on path: " + path + " after retry:" + retryCount + " times");
  }
}
