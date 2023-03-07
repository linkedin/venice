package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.commons.lang3.Validate;


@Threadsafe
public class MergeResultValueSchemaResolverImpl implements MergeResultValueSchemaResolver {
  private final StringAnnotatedStoreSchemaCache storeSchemaCache;
  private final String storeName;
  private final Map<String, SchemaEntry> resolvedSchemaCache;

  public MergeResultValueSchemaResolverImpl(StringAnnotatedStoreSchemaCache storeSchemaCache, String storeName) {
    this.storeSchemaCache = Validate.notNull(storeSchemaCache);
    Validate.notNull(storeName);
    if (storeName.isEmpty()) {
      throw new IllegalArgumentException("Store name should not be an empty String.");
    }
    this.storeName = storeName;
    this.resolvedSchemaCache = new ConcurrentHashMap<>();
  }

  /**
   * For general purpose, refer to Javadoc of {@link MergeResultValueSchemaResolver#getMergeResultValueSchema(int, int)}.
   *
   * This implementation handles 3 situations:
   *
   *    1. Old value schema ID is the same as new value schema ID.
   *          Return either schema.
   *
   *    2. Between old and new value schemas, one is a superset schema of another.
   *          Return the one that is the superset schema.
   *
   *    3. Old and new value schemas mismatch.
   *          Get and return the registered superset schema on this store.
   */
  @Override
  public SchemaEntry getMergeResultValueSchema(final int firstValueSchemaID, final int secondValueSchemaID) {
    if (firstValueSchemaID == secondValueSchemaID) {
      return storeSchemaCache.getValueSchema(secondValueSchemaID);
    }
    final String schemaPairString = createSchemaPairString(firstValueSchemaID, secondValueSchemaID);

    return resolvedSchemaCache.computeIfAbsent(schemaPairString, s -> {
      final SchemaEntry firstValueSchemaEntry = storeSchemaCache.getValueSchema(firstValueSchemaID);
      final SchemaEntry secondValueSchemaEntry = storeSchemaCache.getValueSchema(secondValueSchemaID);
      final Schema firstValueSchema = firstValueSchemaEntry.getSchema();
      final Schema secondValueSchema = secondValueSchemaEntry.getSchema();

      if (AvroSupersetSchemaUtils.isSupersetSchema(secondValueSchema, firstValueSchema)) {
        return secondValueSchemaEntry;
      }
      if (AvroSupersetSchemaUtils.isSupersetSchema(firstValueSchema, secondValueSchema)) {
        return firstValueSchemaEntry;
      }
      // Neither old value schema nor new value schema is the superset schema. So there must be superset schema
      // registered.
      final SchemaEntry registeredSupersetSchema = storeSchemaCache.getSupersetSchema();
      if (registeredSupersetSchema == null) {
        throw new VeniceException("Got null superset schema for store " + storeName);
      }
      validateRegisteredSupersetSchema(registeredSupersetSchema, firstValueSchemaEntry, secondValueSchemaEntry);
      return registeredSupersetSchema;
    });
  }

  private void validateRegisteredSupersetSchema(
      SchemaEntry registeredSupersetSchema,
      SchemaEntry firstValueSchemaEntry,
      SchemaEntry secondValueSchemaEntry) {
    if (!AvroSupersetSchemaUtils
        .isSupersetSchema(registeredSupersetSchema.getSchema(), firstValueSchemaEntry.getSchema())) {
      throw new VeniceException(
          String.format(
              "For store %s, the registered superset schema is NOT a superset schema for "
                  + "a given value schema. Got registered superset schema ID: %d and given value schema ID: %d",
              storeName,
              registeredSupersetSchema.getId(),
              firstValueSchemaEntry.getId()));
    }
    if (!AvroSupersetSchemaUtils
        .isSupersetSchema(registeredSupersetSchema.getSchema(), secondValueSchemaEntry.getSchema())) {
      throw new VeniceException(
          String.format(
              "For store %s, the registered superset schema is NOT a superset schema for "
                  + "a given value schema. Got registered superset schema ID: %d and given value schema ID: %d",
              storeName,
              registeredSupersetSchema.getId(),
              secondValueSchemaEntry.getId()));
    }
  }

  private String createSchemaPairString(final int firstValueSchemaID, final int secondValueSchemaID) {
    return firstValueSchemaID <= secondValueSchemaID
        ? firstValueSchemaID + "-" + secondValueSchemaID
        : secondValueSchemaID + "-" + firstValueSchemaID;
  }
}
