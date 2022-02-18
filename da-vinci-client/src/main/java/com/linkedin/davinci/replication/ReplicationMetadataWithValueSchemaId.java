package com.linkedin.davinci.replication;

import com.linkedin.davinci.replication.merge.MergeConflictResolver;
import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;


/**
 * A POJO class to store Replication Metadata ByteBuffer and the value schema id used to generate the schema for it.
 */
public class ReplicationMetadataWithValueSchemaId {
  private final int valueSchemaId;
  private final GenericRecord replicationMetadataRecord;

  public ReplicationMetadataWithValueSchemaId(int valueSchemaId, GenericRecord replicationMetadataRecord) {
    this.valueSchemaId = valueSchemaId;
    this.replicationMetadataRecord = replicationMetadataRecord;
  }

  public GenericRecord getReplicationMetadataRecord() {
    return replicationMetadataRecord;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }
}
