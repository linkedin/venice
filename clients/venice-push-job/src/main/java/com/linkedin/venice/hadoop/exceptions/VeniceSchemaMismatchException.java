package com.linkedin.venice.hadoop.exceptions;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Customized exception for schema mismatch error during Venice push job. The exception captures the following
 * scenarios where key or value schema of the input data does not match with the expected schema on the venice
 * server.
 */
public class VeniceSchemaMismatchException extends VeniceException {
  public VeniceSchemaMismatchException(String message) {
    super(message);
  }
}
