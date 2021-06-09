package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class PartitionerSchemaMismatchException extends VeniceException {

  public int getHttpStatusCode(){
    return HttpStatus.SC_BAD_REQUEST;
  }

}
