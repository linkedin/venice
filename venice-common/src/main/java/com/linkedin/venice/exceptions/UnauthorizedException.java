package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


/**
 * Thrown when a client doesn't have ACL to a resource.
 */
public class UnauthorizedException extends VeniceException {
  public UnauthorizedException(String s){
    super(s);
  }

  @Override
  public int getHttpStatusCode(){
    return HttpStatus.SC_UNAUTHORIZED;
  }
}
