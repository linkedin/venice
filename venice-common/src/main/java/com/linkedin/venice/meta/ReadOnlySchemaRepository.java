package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.schema.MetadataSchemaEntry;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.MetadataVersionId;
import com.linkedin.venice.schema.SchemaEntry;

import com.linkedin.venice.utils.Pair;
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
   * Get the most recent value schema added to the given store
   */
  SchemaEntry getLatestValueSchema(String storeName);

  /**
   * Look up derived schema id and its corresponding value schema id
   * by given store name and derived schema. This is likely used by
   * clients that write to Venice
   *
   * @return a pair where the first value is value schema id and the
   * second value is derived schema id
   */
  Pair<Integer, Integer> getDerivedSchemaId(String storeName, String derivedSchemaStr);

  DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId);

  Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName);

  /**
   * Get the most recent derived schema added to the given store and value schema id
   */
  DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId);


  MetadataVersionId getMetadataVersionId(String storeName, String metadataSchemaStr);
  MetadataSchemaEntry getMetadataSchema(String storeName, int valueSchemaId, int metadataVersionId);
  Collection<MetadataSchemaEntry> getMetadataSchemas(String storeName);
}
