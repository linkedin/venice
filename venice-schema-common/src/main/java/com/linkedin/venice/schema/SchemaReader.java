package com.linkedin.venice.schema;

import java.io.Closeable;
import org.apache.avro.Schema;


/**
 * This interface is considered as an internal interface to Venice codebase. Venice's users should consider using
 * {@link com.linkedin.venice.client.schema.StoreSchemaFetcher} to fetch Venice store schemas.
 */
public interface SchemaReader extends Closeable {
  Schema getKeySchema();

  Schema getValueSchema(int id);

  int getValueSchemaId(Schema schema);

  Schema getLatestValueSchema();

  Integer getLatestValueSchemaId();
}
