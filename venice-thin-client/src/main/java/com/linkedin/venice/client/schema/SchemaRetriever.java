package com.linkedin.venice.client.schema;

import java.io.Closeable;
import org.apache.avro.Schema;


public interface SchemaRetriever extends Closeable {

  Schema getKeySchema();

  Schema getValueSchema(int id);

  Schema getLatestValueSchema();

  Integer getLatestValueSchemaId();
}
