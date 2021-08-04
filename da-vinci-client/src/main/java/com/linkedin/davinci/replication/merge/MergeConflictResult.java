package com.linkedin.davinci.replication.merge;

import java.nio.ByteBuffer;


/**
 * A encapsulate the results of conflict resolution to denote how the operation and value that should be applied or if
 * the current update should be ignored.
 */
public class MergeConflictResult {
  private static final MergeConflictResult IGNORED_RESULT = new MergeConflictResult();


  private ByteBuffer value;
  private int valueSchemaID;
  private boolean updateIgnored; // Whether we should skip the incoming message since it could be a stale message.
  private ByteBuffer replicationMetadata;
  private boolean resultReusesInput;

  public MergeConflictResult(ByteBuffer value, int valueSchemaID, ByteBuffer replicationMetadata, boolean resultReusesInput) {
    this.updateIgnored = false;
    this.value = value;
    this.valueSchemaID = valueSchemaID;
    this.replicationMetadata = replicationMetadata;
    this.resultReusesInput = resultReusesInput;
  }

  private MergeConflictResult() {
    this.updateIgnored = true;
  }

  public static MergeConflictResult getIgnoredResult() {
    return IGNORED_RESULT;
  }

  public int getValueSchemaID() {
    return this.valueSchemaID;
  }

  public ByteBuffer getValue() {
    return this.value;
  }

  public boolean isUpdateIgnored() {
    return this.updateIgnored;
  }

  public ByteBuffer getReplicationMetadata() {
    return this.replicationMetadata;
  }

  public boolean doesResultReuseInput() {
    return resultReusesInput;
  }
}