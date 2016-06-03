package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import org.apache.avro.Schema;

import java.util.Collection;

public interface ReadOnlySchemaRepository extends VeniceResource, StoreDataChangedListener {

  /**
   * Get key schema for the given store.
   *
   * @param storeName
   * @return
   */
  SchemaEntry getKeySchema(String storeName);

  /**
   * Get value schema for the given store and value schema id.
   *
   * @param storeName
   * @param id
   * @return
   */
  SchemaEntry getValueSchema(String storeName, int id);

  /**
   * Look up the schema id by store name and value schema.
   *
   * @param storeName
   * @param valueSchemaStr
   * @return
   */
  int getValueSchemaId(String storeName, String valueSchemaStr);

  /**
   * Get all the value schemas for the given store.
   *
   * @param storeName
   * @return
   */
  Collection<SchemaEntry> getValueSchemas(String storeName);

}
