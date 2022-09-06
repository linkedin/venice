package com.linkedin.venice.exceptions;

/**
 * Thrown when a config property is invalid or missing
 */
public class ConfigurationException extends VeniceException {
  private static final long serialVersionUID = 1L;

  public ConfigurationException(String message) {
    super(message);
    super.errorType = ErrorType.INVALID_CONFIG;
  }

  public ConfigurationException(Exception cause) {
    super(cause);
    super.errorType = ErrorType.INVALID_CONFIG;
  }

  public ConfigurationException(String message, Exception cause) {
    super(message, cause);
    super.errorType = ErrorType.INVALID_CONFIG;
  }
}
