package com.linkedin.venice.exceptions;

/**
 * Base exception that all other Venice exceptions extend
 */
public class VeniceException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  protected ErrorType errorType = ErrorType.GENERAL_ERROR;

  public VeniceException() {
    super();
  }

  public VeniceException(String s) {
    super(s);
  }

  public VeniceException(String s, boolean fillInStacktrace) {
    super(s, null, true, fillInStacktrace);
  }

  public VeniceException(String s, ErrorType errorType) {
    super(s);
    this.errorType = errorType;
  }

  public VeniceException(Throwable t) {
    super(t);
  }

  public VeniceException(String s, Throwable t) {
    super(s, t);
  }

  public VeniceException(String s, Throwable t, ErrorType errorType) {
    super(s, t);
    this.errorType = errorType;
  }

  /**
   * If this exception is caught in handling an http request, what status code should be returned?
   * By default this is derived from the {@link ErrorType} set on the exception (see
   * {@link ErrorType#getHttpStatusCode()}), which defaults to 500 (Internal Server Error) for
   * {@link ErrorType#GENERAL_ERROR}. Exceptions that extend VeniceException can override this for different behavior.
   */
  public int getHttpStatusCode() {
    return errorType.getHttpStatusCode();
  }

  /**
   * Returns the errorType.  Extenders of this class should fill in the errorType member
   * @return
   */
  public final ErrorType getErrorType() {
    return errorType;
  }
}
