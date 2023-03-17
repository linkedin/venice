package com.linkedin.venice.schema;

import static com.linkedin.venice.schema.SchemaData.INVALID_VALUE_SCHEMA_ID;


/**
 * In Venice, some schemas are generated, by deriving them from value schemas. These generated schemas
 * are identified by a composite ID:
 *
 * - The {@link #valueSchemaID}, which is the value schema that it was generated from.
 * - The {@link #generatedSchemaVersion}, which serves to version the generation logic itself, to account for
 *   future changes to how the generation is performed, yielding a different generated schema.
 *
 * Examples of generated schemas include:
 *
 * - Write compute operation schema ({@link com.linkedin.venice.schema.writecompute.DerivedSchemaEntry})
 * - Replication metadata schema ({@link com.linkedin.venice.schema.rmd.RmdSchemaEntry})
 */
public class GeneratedSchemaID {
  public static final GeneratedSchemaID INVALID =
      new GeneratedSchemaID(INVALID_VALUE_SCHEMA_ID, INVALID_VALUE_SCHEMA_ID);

  private final int valueSchemaID;
  private final int generatedSchemaVersion;

  public GeneratedSchemaID(int valueSchemaID, int generatedSchemaVersion) {
    this.valueSchemaID = valueSchemaID;
    this.generatedSchemaVersion = generatedSchemaVersion;
  }

  public int getValueSchemaID() {
    return valueSchemaID;
  }

  public int getGeneratedSchemaVersion() {
    return generatedSchemaVersion;
  }

  public boolean isValid() {
    return valueSchemaID != INVALID_VALUE_SCHEMA_ID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GeneratedSchemaID that = (GeneratedSchemaID) o;

    if (valueSchemaID != that.valueSchemaID) {
      return false;
    }
    return generatedSchemaVersion == that.generatedSchemaVersion;
  }

  @Override
  public int hashCode() {
    int result = valueSchemaID;
    result = 31 * result + generatedSchemaVersion;
    return result;
  }
}
