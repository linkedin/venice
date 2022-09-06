package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class PartitionerSchemaMismatchException extends VeniceException {
  public PartitionerSchemaMismatchException() {
    super();
  }

  public PartitionerSchemaMismatchException(String s) {
    super(s);
  }

  public PartitionerSchemaMismatchException(Throwable t) {
    super(t);
  }

  public PartitionerSchemaMismatchException(String s, Throwable t) {
    super(s, t);
  }

  public int getHttpStatusCode() {
    return HttpStatus.SC_BAD_REQUEST;
  }

}
