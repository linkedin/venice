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

  private ByteBuffer newValue;
  private Optional<GenericRecord> deserializedValue;
  private int valueSchemaId;
  private final boolean updateIgnored; // Whether we should skip the incoming message since it could be a stale message.
  private boolean resultReusesInput;
  private GenericRecord rmdRecord;

  public MergeConflictResult(
      ByteBuffer newValue,
      int valueSchemaID,
      boolean resultReusesInput,
      GenericRecord rmdRecord) {
    this(newValue, Optional.empty(), valueSchemaID, resultReusesInput, rmdRecord);
  }

  public MergeConflictResult(
      ByteBuffer newValue,
      Optional<GenericRecord> deserializedValue,
      int valueSchemaID,
      boolean resultReusesInput,
      GenericRecord rmdRecord) {
    this.updateIgnored = false;
    this.newValue = newValue;
    this.deserializedValue = deserializedValue;
    this.valueSchemaId = valueSchemaID;
    this.resultReusesInput = resultReusesInput;
    this.rmdRecord = rmdRecord;
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

  public ByteBuffer getNewValue() {
    return this.newValue;
  }

  public boolean isUpdateIgnored() {
    return this.updateIgnored;
  }

  public boolean doesResultReuseInput() {
    return resultReusesInput;
  }

  public GenericRecord getRmdRecord() {
    return rmdRecord;
  }

  /**
   * Provide the deserialized new value on a best-effort approach. Meaning that it's acceptable to return an empty
   * Optional. e.g. MergeConflictResult of full PUTs will not contain deserialized new value since we don't need to
   * deserialize the value to generate the MCR.
   * @return deserialized new value if possible.
   */
  public Optional<GenericRecord> getDeserializedValue() {
    return deserializedValue;
  }
}
