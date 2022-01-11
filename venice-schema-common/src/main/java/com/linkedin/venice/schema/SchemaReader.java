package com.linkedin.venice.schema;

import java.io.Closeable;
import org.apache.avro.Schema;

public interface SchemaReader extends Closeable {
  Schema getKeySchema();

  Schema getValueSchema(int id);

  int getValueSchemaId(Schema schema);

  Schema getLatestValueSchema();

  Integer getLatestValueSchemaId();
}
