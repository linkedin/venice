package com.linkedin.venice.client.schema;

import java.io.Closeable;
import org.apache.avro.Schema;


public interface SchemaRetriever extends Closeable {

  Schema getKeySchema();

  Schema getValueSchema(int id);

  int getValueSchemaId(Schema schema);

  Schema getLatestValueSchema();

  Integer getLatestValueSchemaId();
}
