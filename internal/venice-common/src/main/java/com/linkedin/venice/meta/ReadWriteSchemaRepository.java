package com.linkedin.venice.meta;

import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;


public interface ReadWriteSchemaRepository extends ReadOnlySchemaRepository {
  /**
   * Set up key schema for the given store
   */
  SchemaEntry initKeySchema(String storeName, String schemaStr);

  /**
   * Add a new value schema for the given store
   */
  default SchemaEntry addValueSchema(String storeName, String schemaStr) {
    return addValueSchema(storeName, schemaStr, DirectionalSchemaCompatibilityType.FULL);
  }

  /**
   * Add a new value schema for the given store
   */
  SchemaEntry addValueSchema(
      String storeName,
      String schemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType);

  /**
   * Add a new value schema for the given store by specifying schema id.
   * This API is mostly intended to be used in cross-colo mode. When there
   * are multiple colos, we'd like to have consistent value id across colos,
   * so that deserializer can work properly while reading records.
   *
   * Caller should figure out the schema id number by themselves.
   * TODO: Might want to remove it from the interface and make it invisible from the outside
   */
  SchemaEntry addValueSchema(String storeName, String schemaStr, int schemaId);

  /**
   * Add a new derived schema for the given store and value schema id
   */
  DerivedSchemaEntry addDerivedSchema(String storeName, String schemaStr, int valueSchemaId);

  /**
   * Add a new derived schema for the given store by specifying derived
   * schema id. Mostly used in cross-colo mode.
   */
  DerivedSchemaEntry addDerivedSchema(String storeName, String schemaStr, int valueSchemaId, int derivedSchemaId);

  /**
   * Remove an existing derived schema
   * @return the derived schema that is deleted or null if the schema doesn't exist
   */
  DerivedSchemaEntry removeDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId);

  int preCheckValueSchemaAndGetNextAvailableId(
      String storeName,
      String valueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType);

  int preCheckDerivedSchemaAndGetNextAvailableId(String storeName, int valueSchemaId, String derivedSchemaStr);

  RmdSchemaEntry addReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      String replicationMetadataSchemaStr,
      int replicationMetadataVersionId);
}
