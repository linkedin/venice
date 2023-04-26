package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import java.util.Collection;


public interface ReadOnlySchemaRepository extends VeniceResource {
  /**
   * Get key schema for the given store.
   */
  SchemaEntry getKeySchema(String storeName);

  /**
   * Get value schema for the given store and value schema id.
   */
  SchemaEntry getValueSchema(String storeName, int id);

  /**
   * Check whether the specified schema id is valid or not
   */
  boolean hasValueSchema(String storeName, int id);

  /**
   * Look up the schema id by store name and value schema.
   */
  int getValueSchemaId(String storeName, String valueSchemaStr);

  /**
   * Get all the value schemas for the given store.
   */
  Collection<SchemaEntry> getValueSchemas(String storeName);

  /**
   * Get the most recent value schema or superset value schema if one exists.
   */
  SchemaEntry getSupersetOrLatestValueSchema(String storeName);

  /**
   * Get the superset value schema for a given store. Each store has at most one active superset schema. Specifically a
   * store must have some features enabled (e.g. read compute, write compute) to have a superset value schema which
   * evolves as new value schemas are added.
   *
   * @return Superset value schema or {@code null} if store {@param storeName} does not have any superset value schema.
   */
  SchemaEntry getSupersetSchema(String storeName);

  /**
   * Look up derived schema id and its corresponding value schema id by given store name and derived schema. This is
   * likely used by clients that write to Venice
   *
   * @return a pair where the first value is value schema id and the second value is derived schema id
   */
  GeneratedSchemaID getDerivedSchemaId(String storeName, String derivedSchemaStr);

  DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int writeComputeSchemaId);

  Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName);

  /**
   * Get the most recent derived schema added to the given store and value schema id
   */
  DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId);

  RmdSchemaEntry getReplicationMetadataSchema(String storeName, int valueSchemaId, int replicationMetadataVersionId);

  Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName);
}
