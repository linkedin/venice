package com.linkedin.venice.utils;

/**
 *  Thrown when a required property is not present
 */
public class UndefinedPropertyException extends ConfigurationException {
  private static final long serialVersionUID = 1;

  public UndefinedPropertyException(String variable) {
    super("Missing required property '" + variable + "'.");
  }
}
