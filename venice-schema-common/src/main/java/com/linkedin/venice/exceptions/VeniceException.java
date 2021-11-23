package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


/**
 * Base exception that all other Venice exceptions extend
 */
public class VeniceException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  protected ExceptionType exceptionType = ExceptionType.GENERAL_ERROR;

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

  public VeniceException(String s, Throwable t, ExceptionType exceptionType){
    super(s,t);
    this.exceptionType = exceptionType;
  }

  /**
   * If this exception is caught in handling an http request, what status code should be returned?
   * Exceptions that extend VeniceException can override this for different behavior
   * @return 500 (Internal Server Error)
   */
  public int getHttpStatusCode(){
    return HttpStatus.SC_INTERNAL_SERVER_ERROR;
  }

  /**
   * Returns the exceptionType.  Extenders of this class should fill in the exceptionType member
   * @return
   */
  public final ExceptionType getExceptionType() {
    return exceptionType;
  }

}
