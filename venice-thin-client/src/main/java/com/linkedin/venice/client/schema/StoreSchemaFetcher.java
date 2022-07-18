package com.linkedin.venice.client.schema;

import java.io.Closeable;
import org.apache.avro.Schema;


/**
 * This class is the public interface for Venice store user to fetch store schemas. It is intended to
 * replace the old schema fetching APIs in {@link com.linkedin.venice.client.store.AvroGenericStoreClient}.
 * It is expected that each instance of this class is associated with a single Venice store and user can use it to
 * fetch the Venice store schemas for their logics.
 */
public interface StoreSchemaFetcher extends Closeable {
  /**
   * Returns KEY schema of the store.
   */
  Schema getKeySchema();

  /**
   * Returns the latest available VALUE schema of the store.
   */
  Schema getLatestValueSchema();

  /**
   * Returns the latest available UPDATE (write compute) schema of the store.
   */
  Schema getLatestUpdateSchema();

  /**
   * Returns the Venice store name this class is associated with.
   */
  String getStoreName();
}
