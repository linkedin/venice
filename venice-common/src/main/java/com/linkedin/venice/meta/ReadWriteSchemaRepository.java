package com.linkedin.venice.meta;

import com.linkedin.venice.schema.SchemaEntry;

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
  SchemaEntry addValueSchema(String storeName, String schemaStr);
}
