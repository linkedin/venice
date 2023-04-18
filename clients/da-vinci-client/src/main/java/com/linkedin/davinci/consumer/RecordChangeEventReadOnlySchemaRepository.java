package com.linkedin.davinci.consumer;

import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.util.Arrays;
import java.util.Collection;


/**
 * TODO: This class is a bit of a hack.  We need to add functionality into the server
 * to be able to query and retrieve schemas for different view types.  But this functionality isn't
 * available today.  This class right now operates a shim interface to retrieve the right schema,
 * but it runs the risk of the protocol not being easily evolvable.
 */
public class RecordChangeEventReadOnlySchemaRepository implements ReadOnlySchemaRepository {
  ReadOnlySchemaRepository internalReadOnlySchemaRepository;
  private static final SchemaEntry SCHEMA = new SchemaEntry(
      AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersion(),
      RecordChangeEvent.getClassSchema());

  RecordChangeEventReadOnlySchemaRepository(ReadOnlySchemaRepository internalReadOnlySchemaRepository) {
    this.internalReadOnlySchemaRepository = internalReadOnlySchemaRepository;
  }

  @Override
  public void refresh() {
    this.internalReadOnlySchemaRepository.refresh();
  }

  @Override
  public void clear() {
    this.internalReadOnlySchemaRepository.clear();
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    return this.internalReadOnlySchemaRepository.getKeySchema(storeName);
  }

  @Override
  public SchemaEntry getValueSchema(String storeName, int id) {
    return SCHEMA;
  }

  @Override
  public boolean hasValueSchema(String storeName, int id) {
    return true;
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    return AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersion();
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    return Arrays.asList(SCHEMA);
  }

  @Override
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    return SCHEMA;
  }

  @Override
  public SchemaEntry getSupersetSchema(String storeName) {
    return SCHEMA;
  }

  @Override
  public GeneratedSchemaID getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int writeComputeSchemaId) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  @Override
  public RmdSchemaEntry getReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId) {
    return internalReadOnlySchemaRepository
        .getReplicationMetadataSchema(storeName, valueSchemaId, replicationMetadataVersionId);
  }

  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName) {
    return internalReadOnlySchemaRepository.getReplicationMetadataSchemas(storeName);
  }
}
