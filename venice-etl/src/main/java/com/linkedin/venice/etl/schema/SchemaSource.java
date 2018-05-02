package com.linkedin.venice.etl.schema;

import org.apache.avro.Schema;


public interface SchemaSource {
  public Schema fetchKeySchema(String storeName);
  public Schema[] fetchValueSchemas(String storeName);
}
