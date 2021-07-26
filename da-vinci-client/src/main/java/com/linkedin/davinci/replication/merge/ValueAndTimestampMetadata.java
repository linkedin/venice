package com.linkedin.davinci.replication.merge;

import org.apache.avro.generic.GenericRecord;


/**
 * Wrapper class to hold a pair of {@link GenericRecord}, including a value and its corresponding
 * timestamp metadata.
 */
public class ValueAndTimestampMetadata<T> {
  private T value;
  private GenericRecord timestampMetadata;
  boolean updateIgnored; // Whether we should skip the incoming message since it could be a stale message.
  int resolvedSchemaID;

  public ValueAndTimestampMetadata(T value, GenericRecord timestampMetadata) {
    this.value = value;
    this.timestampMetadata = timestampMetadata;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public GenericRecord getTimestampMetadata() {
    return timestampMetadata;
  }

  public void setTimestampMetadata(GenericRecord timestampMetadata) {
    this.timestampMetadata = timestampMetadata;
  }

  public void setUpdateIgnored(boolean updateIgnored) {
    this.updateIgnored = updateIgnored;
  }

  public boolean isUpdateIgnored() {
    return updateIgnored;
  }

  public void setResolvedSchemaID(int schemaID) {
    this.resolvedSchemaID = schemaID;
  }

  public int getResolvedSchemaID() {
    return this.resolvedSchemaID;
  }
}
