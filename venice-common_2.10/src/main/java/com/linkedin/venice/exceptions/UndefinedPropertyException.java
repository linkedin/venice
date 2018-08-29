package com.linkedin.venice.exceptions;

import com.linkedin.venice.exceptions.ConfigurationException;


/**
 *  Thrown when a required property is not present
 */
public class UndefinedPropertyException extends ConfigurationException {
  private static final long serialVersionUID = 1L;

  public UndefinedPropertyException(String variable) {
    super("Missing required property '" + variable + "'.");
  }
}
