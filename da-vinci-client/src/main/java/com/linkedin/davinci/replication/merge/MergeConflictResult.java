package com.linkedin.davinci.replication.merge;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;


/**
 * An object to encapsulate the results of conflict resolution to denote how the operation and value that should be
 * applied or if the current update should be ignored.
 */
public class MergeConflictResult {
  private static final MergeConflictResult IGNORED_RESULT = new MergeConflictResult();

  private Optional<ByteBuffer> newValue;
  private int valueSchemaId;
  private final boolean updateIgnored; // Whether we should skip the incoming message since it could be a stale message.
  private boolean resultReusesInput;
  private GenericRecord replicationMetadataRecord;

  public MergeConflictResult(Optional<ByteBuffer> newValue, int valueSchemaID, boolean resultReusesInput, GenericRecord replicationMetadataRecord) {
    this.updateIgnored = false;
    this.newValue = newValue;
    this.valueSchemaId = valueSchemaID;
    this.resultReusesInput = resultReusesInput;
    this.replicationMetadataRecord = replicationMetadataRecord;
  }

  private MergeConflictResult() {
    this.updateIgnored = true;
  }

  public static MergeConflictResult getIgnoredResult() {
    return IGNORED_RESULT;
  }

  public int getValueSchemaId() {
    return this.valueSchemaId;
  }

  public Optional<ByteBuffer> getNewValue() {
    return this.newValue;
  }

  public boolean isUpdateIgnored() {
    return this.updateIgnored;
  }

  public boolean doesResultReuseInput() {
    return resultReusesInput;
  }

  public GenericRecord getReplicationMetadataRecord() {
    return replicationMetadataRecord;
  }
}
