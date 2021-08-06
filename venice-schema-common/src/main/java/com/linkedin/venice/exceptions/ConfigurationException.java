package com.linkedin.venice.exceptions;

/**
 * Thrown when a config property is invalid or missing
 */
public class ConfigurationException extends VeniceException {

  final static long serialVersionUID = 1L;
  protected ExceptionType exceptionType = ExceptionType.INVALID_CONFIG;


  public ConfigurationException(String message) {
    super(message);
  }

  public ConfigurationException(String message, Exception cause) {
    super(message, cause);
  }

  public ConfigurationException(Exception cause) {
    super(cause);
  }
}
