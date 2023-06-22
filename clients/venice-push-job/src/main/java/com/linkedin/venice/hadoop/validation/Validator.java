package com.linkedin.venice.hadoop.validation;

import com.linkedin.venice.hadoop.exceptions.VeniceValidationException;


/**
 * Interface for targeted region push validation.
 */
public interface Validator {
  void validate() throws VeniceValidationException;
}
