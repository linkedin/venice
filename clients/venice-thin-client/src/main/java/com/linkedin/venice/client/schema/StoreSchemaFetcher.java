package com.linkedin.venice.client.schema;

import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.Closeable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class is the public interface for Venice store user to fetch store schemas. It is intended to
 * replace the old schema fetching APIs in {@link com.linkedin.venice.client.store.AvroGenericStoreClient}.
 * It is expected that each instance of this class is associated with a single Venice store and user can use it to
 * fetch the Venice store schemas for their logics.
 */
public interface StoreSchemaFetcher extends Closeable {
  /**
   * Returns Key schema of the store.
   */
  Schema getKeySchema();

  /**
   * Returns the latest available Value schema of the store.
   */
  Schema getLatestValueSchema();

  /**
   * Get the superset value schema for a given store. Each store has at most one active superset schema. Specifically a
   * store must have some features enabled (e.g. read compute, write compute) to have a superset value schema which
   * evolves as new value schemas are added.
   *
   * @return Superset value schema or {@code null} if store {@param storeName} does not have any superset value schema.
   */
  Schema getSupersetSchema();

  /**
   * Returns the Update (Write Compute) schema of the provided Value schema. The returned schema is used to construct
   * a {@link GenericRecord} that partially updates a record value that is associated with this value schema.
   * This method is expected to throw {@link InvalidVeniceSchemaException} when the provided value schema could not be
   * found in the Venice backend.
   * For other unexpected errors like store is not write-compute enabled or corresponding update schema is not found
   * in the Venice backend, this method will throw general {@link VeniceException} with corresponding error message.
   */
  Schema getUpdateSchema(Schema valueSchema) throws VeniceException;

  /**
   * Returns the Venice store name this class is associated with.
   */
  String getStoreName();
}
