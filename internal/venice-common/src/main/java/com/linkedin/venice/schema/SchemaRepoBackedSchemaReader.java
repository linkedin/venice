package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import java.util.Collection;
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
  public Schema getUpdateSchema(int valueSchemaId) {
    Collection<DerivedSchemaEntry> derivedSchemas = schemaRepository.getDerivedSchemas(storeName);

    if (derivedSchemas.isEmpty()) {
      throw new VeniceException(
          "No update schemas are available for the store " + storeName
              + ". Check if the store is configured correctly.");
    }

    return derivedSchemas.stream()
        .filter(derivedSchemaEntry -> derivedSchemaEntry.getValueSchemaID() == valueSchemaId)
        .max(Comparator.comparingInt(SchemaEntry::getId))
        .map(DerivedSchemaEntry::getSchema)
        .orElse(null);
  }

  @Override
  public DerivedSchemaEntry getLatestUpdateSchema() {
    Collection<DerivedSchemaEntry> derivedSchemas = schemaRepository.getDerivedSchemas(storeName);

    if (derivedSchemas.isEmpty()) {
      throw new VeniceException(
          "No update schemas are available for the store " + storeName
              + ". Check if the store is configured correctly.");
    }

    int latestValueSchemaId = getLatestValueSchemaId();

    return derivedSchemas.stream()
        .filter(derivedSchemaEntry -> derivedSchemaEntry.getValueSchemaID() == latestValueSchemaId)
        .max(Comparator.comparingInt(SchemaEntry::getId))
        .orElse(null);
  }
}
