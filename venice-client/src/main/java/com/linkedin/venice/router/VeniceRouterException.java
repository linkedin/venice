package com.linkedin.venice.router;

public class VeniceRouterException extends RuntimeException{ //extends RouterException {
  public VeniceRouterException(String msg, Throwable e){
    super(msg, e);
  }
  public VeniceRouterException(String msg){
    super(msg);
  }
}
