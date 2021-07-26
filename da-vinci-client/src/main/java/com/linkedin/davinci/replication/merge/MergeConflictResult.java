package com.linkedin.davinci.replication.merge;

import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;


public class MergeConflictResult {
  private ByteBuffer resultByteBuffer;
  private int resultSchemaID;
  private boolean updateIgnored; // Whether we should skip the incoming message since it could be a stale message.
  private GenericRecord replicationMetadata;

  public void setReplicationMetadata(GenericRecord replicationMetadata) {
    this.replicationMetadata = replicationMetadata;
  }

  public void setResultByteBuffer(ByteBuffer resultByteBuffer) {
    this.resultByteBuffer = resultByteBuffer;
  }

  public void setResultSchemaID(int resultSchemaID) {
    this.resultSchemaID = resultSchemaID;
  }

  public void setUpdateIgnored(boolean updateIgnored) {
    this.updateIgnored = updateIgnored;
  }

  public int getResultSchemaID() {
    return this.resultSchemaID;
  }

  public ByteBuffer getResultByteBuffer() {
    return this.resultByteBuffer;
  }

  public boolean isUpdateIgnored() {
    return this.updateIgnored;
  }

  public GenericRecord getReplicationMetadata() {
    return this.replicationMetadata;
  }
}
