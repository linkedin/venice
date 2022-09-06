package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.schema.SchemaEntry;


/**
 * This interface decides/resolves which schema to use given the schema ID of the existing/old value and the schema ID
 * of the new value.
 */
@Threadsafe
interface MergeResultValueSchemaResolver {
  /**
   * It decides/resolves which schema to use given two schema IDs of two values that are going to be merged. Calling this
   * method with different order of these 2 schema IDs as input should return the same result. Specifically:
   *    getSchemaForMergedValue(a, b) == getSchemaForMergedValue(b, a)
   *
   * @param firstValueSchemaID Self-explanatory
   * @param secondValueSchemaID Self-explanatory
   * @return A schema that should be used for the merged result.
   */
  SchemaEntry getMergeResultValueSchema(final int firstValueSchemaID, final int secondValueSchemaID);
}
