package com.linkedin.venice.hadoop.exceptions;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;


/**
 * Customized exception for non-existing key/value field error in Avro schema
 * in {@link VenicePushJob}
 */
public class VeniceSchemaFieldNotFoundException extends VeniceException {
  private String fieldName;

  public VeniceSchemaFieldNotFoundException(String fieldName, String message) {
    super(message);
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return fieldName;
  }
}
