package com.linkedin.venice.schema;

import static com.linkedin.venice.schema.SchemaData.INVALID_VALUE_SCHEMA_ID;


public class GeneratedSchemaID {
  public static final GeneratedSchemaID INVALID =
      new GeneratedSchemaID(INVALID_VALUE_SCHEMA_ID, INVALID_VALUE_SCHEMA_ID);

  private final int valueSchemaID;
  private final int generatedSchemaID;

  public GeneratedSchemaID(int valueSchemaID, int generatedSchemaID) {
    this.valueSchemaID = valueSchemaID;
    this.generatedSchemaID = generatedSchemaID;
  }

  public int getValueSchemaID() {
    return valueSchemaID;
  }

  public int getGeneratedSchemaID() {
    return generatedSchemaID;
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
    return generatedSchemaID == that.generatedSchemaID;
  }

  @Override
  public int hashCode() {
    int result = valueSchemaID;
    result = 31 * result + generatedSchemaID;
    return result;
  }
}
