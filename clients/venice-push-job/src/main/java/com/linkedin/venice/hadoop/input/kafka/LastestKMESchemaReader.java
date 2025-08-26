package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import java.io.IOException;
import org.apache.avro.Schema;


public class LastestKMESchemaReader implements SchemaReader {
  private Schema lastestKMESchema;

  public LastestKMESchemaReader(Schema lastestKMESchema) {
    this.lastestKMESchema = lastestKMESchema;
  }

  @Override
  public Schema getKeySchema() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Schema getValueSchema(int id) {
    return lastestKMESchema;
  }

  @Override
  public int getValueSchemaId(Schema schema) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Schema getLatestValueSchema() {
    return lastestKMESchema;
  }

  @Override
  public Integer getLatestValueSchemaId() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Schema getUpdateSchema(int valueSchemaId) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public DerivedSchemaEntry getLatestUpdateSchema() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() throws IOException {
  }
}
