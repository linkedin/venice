package com.linkedin.venice.exceptions;

import com.linkedin.venice.schema.SchemaEntry;


/**
 * Throw this exception when the schema is not fully compatible with the previous ones.
 */
public class SchemaIncompatibilityException extends VeniceException {
  // TODO: produce more accurate reason for incompatible schemas
  public SchemaIncompatibilityException(SchemaEntry sourceSchema, SchemaEntry targetSchema) {
    super(
        "New schema is not fully compatible with the previous schema.\nOld schema: " + sourceSchema.toString(true)
            + "\nNew schema: " + targetSchema.toString(true));
    super.errorType = ErrorType.INVALID_SCHEMA;
  }
}
