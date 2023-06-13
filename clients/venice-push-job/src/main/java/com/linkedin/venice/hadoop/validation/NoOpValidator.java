package com.linkedin.venice.hadoop.validation;

import com.linkedin.venice.hadoop.exceptions.VeniceValidationException;


/**
 * No Op validator. Always returns true.
 */
public class NoOpValidator implements Validator {
  @Override
  public void validate() throws VeniceValidationException {
    // No Op
    return;
  }
}
