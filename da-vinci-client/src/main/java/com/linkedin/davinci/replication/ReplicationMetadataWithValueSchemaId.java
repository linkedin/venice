package com.linkedin.davinci.replication;

import org.apache.avro.generic.GenericRecord;


/**
 * A POJO class that contains 3 things/fields:
 *    1. RMD record.
 *    2. RMD protocol version ID.
 *    3. Value schema ID used to generate the RMD schema.
 */
public class ReplicationMetadataWithValueSchemaId {
  private final int valueSchemaID;
  private final int rmdProtocolVersionID; // Note that it is NOT RMD schema ID which should have the format "<valueSchemaId>-<rmdProtocolVersionID>"
  private final GenericRecord replicationMetadataRecord;

  public ReplicationMetadataWithValueSchemaId(int valueSchemaID, int rmdProtocolVersionID, GenericRecord replicationMetadataRecord) {
    this.valueSchemaID = valueSchemaID;
    this.replicationMetadataRecord = replicationMetadataRecord;
    this.rmdProtocolVersionID = rmdProtocolVersionID;
  }

  public GenericRecord getReplicationMetadataRecord() {
    return replicationMetadataRecord;
  }

  public int getValueSchemaId() {
    return valueSchemaID;
  }

  public int getRmdProtocolVersionID() {
    return rmdProtocolVersionID;
  }
}
