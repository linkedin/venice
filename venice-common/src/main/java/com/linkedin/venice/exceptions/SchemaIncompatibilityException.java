package com.linkedin.venice.exceptions;

import com.linkedin.venice.schema.SchemaEntry;

/**
 * Throw this exception when the schema is not fully compatible with the previous ones.
 */
public class SchemaIncompatibilityException extends VeniceException {
  public SchemaIncompatibilityException(SchemaEntry sourceSchema, SchemaEntry targetSchema) {
    super("New schema: " + targetSchema.toString() + " is not fully compatible with existing schema: " + sourceSchema.toString());
  }
}
