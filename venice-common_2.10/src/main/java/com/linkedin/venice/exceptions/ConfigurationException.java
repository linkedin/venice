package com.linkedin.venice.exceptions;

/**
 * Thrown when a config property is invalid or missing
 */
public class ConfigurationException extends RuntimeException {

  final static long serialVersionUID = 1L;

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
