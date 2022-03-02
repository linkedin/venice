package com.linkedin.davinci.replication;

import org.apache.avro.generic.GenericRecord;


/**
 * A POJO class to store RMD and the value schema ID used to generate the RMD schema.
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
