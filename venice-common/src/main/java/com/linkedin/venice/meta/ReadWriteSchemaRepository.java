package com.linkedin.venice.meta;

import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;


public interface ReadWriteSchemaRepository extends ReadOnlySchemaRepository {
  /**
   * Set up key schema for the given store
   *
   * @param storeName
   * @param schemaStr
   */
  SchemaEntry initKeySchema(String storeName, String schemaStr);

  /**
   * Add a new value schema for the given store
   *
   * @param storeName
   * @param schemaStr
   * @return
   */
  default SchemaEntry addValueSchema(String storeName, String schemaStr) {
    return addValueSchema(storeName, schemaStr, DirectionalSchemaCompatibilityType.FULL);
  }

  /**
   * Add a new value schema for the given store by specifying schema id
   *
   * @param storeName
   * @param schemaStr
   * @param schemaId
   * @return
   */
  default SchemaEntry addValueSchema(String storeName, String schemaStr, int schemaId) {
    return addValueSchema(storeName, schemaStr, schemaId, DirectionalSchemaCompatibilityType.FULL);
  }

  /**
   * Add a new value schema for the given store
   *
   * @param storeName
   * @param schemaStr
   * @return
   */
  SchemaEntry addValueSchema(String storeName, String schemaStr, DirectionalSchemaCompatibilityType expectedCompatibilityType);

  /**
   * Add a new value schema for the given store by specifying schema id
   *
   * @param storeName
   * @param schemaStr
   * @param schemaId
   * @return
   */
  SchemaEntry addValueSchema(String storeName, String schemaStr, int schemaId, DirectionalSchemaCompatibilityType expectedCompatibilityType);
}
