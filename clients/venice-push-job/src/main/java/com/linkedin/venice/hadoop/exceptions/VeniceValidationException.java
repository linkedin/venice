package com.linkedin.venice.hadoop.exceptions;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;


/**
 * Customized exception for validation error for targeted colo push
 * in {@link VenicePushJob}
 */
public class VeniceValidationException extends VeniceException {
  public VeniceValidationException(String message) {
    super(message);
  }

  public VeniceValidationException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
