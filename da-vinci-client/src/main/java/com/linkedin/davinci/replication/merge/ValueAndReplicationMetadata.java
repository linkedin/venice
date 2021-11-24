package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.utils.Utils;
import org.apache.avro.generic.GenericRecord;


/**
 * Wrapper class to hold a pair of {@link GenericRecord}, including a value and its corresponding
 * replication metadata.
 */
public class ValueAndReplicationMetadata<T> {
  private T value;
  private GenericRecord replicationMetadata;
  private boolean updateIgnored; // Whether we should skip the incoming message since it could be a stale message.
  private int resolvedSchemaID;

  public ValueAndReplicationMetadata() {
    this.value = null;
    this.replicationMetadata = null;
  }

  public ValueAndReplicationMetadata(T value, GenericRecord replicationMetadata) {
    this.value = Utils.notNull(value);
    this.replicationMetadata = Utils.notNull(replicationMetadata);
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public GenericRecord getReplicationMetadata() {
    return replicationMetadata;
  }

  public void setReplicationMetadata(GenericRecord replicationMetadata) {
    this.replicationMetadata = replicationMetadata;
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
