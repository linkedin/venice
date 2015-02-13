package com.linkedin.venice.exceptions;

/**
 * Base exception that all other Venice exceptions extend
 */
public class VeniceException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public VeniceException(){
    super();
  }

  public VeniceException(String s){
    super(s);
  }

  public VeniceException(Throwable t){
    super(t);
  }
  public VeniceException(String s, Throwable t){
    super(s,t);
  }

}
