package com.linkedin.venice.schema;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import java.util.Comparator;
import org.apache.avro.Schema;


public class SchemaRepoBackedSchemaReader implements SchemaReader {
  private final ReadOnlySchemaRepository schemaRepository;
  private final String storeName;

  public SchemaRepoBackedSchemaReader(ReadOnlySchemaRepository schemaRepository, String storeName) {
    this.schemaRepository = schemaRepository;
    this.storeName = storeName;
  }

  @Override
  public void close() {
    // Nothing to clean up
  }

  @Override
  public Schema getKeySchema() {
    return schemaRepository.getKeySchema(storeName).getSchema();
  }

  @Override
  public Schema getValueSchema(int id) {
    SchemaEntry valueSchemaEntry = schemaRepository.getValueSchema(storeName, id);
    if (valueSchemaEntry == null) {
      return null;
    }
    return valueSchemaEntry.getSchema();
  }

  @Override
  public int getValueSchemaId(Schema schema) {
    return schemaRepository.getValueSchemaId(storeName, schema.toString());
  }

  @Override
  public Schema getLatestValueSchema() {
    return schemaRepository.getSupersetOrLatestValueSchema(storeName).getSchema();
  }

  @Override
  public Integer getLatestValueSchemaId() {
    return schemaRepository.getSupersetOrLatestValueSchema(storeName).getId();
  }

  @Override
  public int getLargestValueSchemaId() {
    return schemaRepository.getValueSchemas(storeName)
        .stream()
        .map(SchemaEntry::getId)
        .max(Comparator.naturalOrder())
        .orElse(SchemaData.INVALID_VALUE_SCHEMA_ID);
  }
}
