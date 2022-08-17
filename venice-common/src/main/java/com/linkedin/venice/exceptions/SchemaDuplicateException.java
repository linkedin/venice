package com.linkedin.venice.exceptions;

import com.linkedin.venice.schema.SchemaEntry;


public class SchemaDuplicateException extends VeniceException {
  public SchemaDuplicateException(SchemaEntry sourceSchemaEntry, SchemaEntry targetSchemaEntry) {
    super(
        "New schema is duplicate with the previous schema. Previous schema: " + sourceSchemaEntry + ", new schema"
            + targetSchemaEntry);
  }
}
