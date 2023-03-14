package com.linkedin.davinci.replication;

import org.apache.avro.generic.GenericRecord;


/**
 * A POJO class that contains 3 things/fields:
 *    1. RMD record.
 *    2. RMD protocol version ID.
 *    3. Value schema ID used to generate the RMD schema.
 */
public class RmdWithValueSchemaId {
  private final int valueSchemaID;
  private final int rmdProtocolVersionID;
  private final GenericRecord rmdRecord;

  public RmdWithValueSchemaId(int valueSchemaID, int rmdProtocolVersionID, GenericRecord rmdRecord) {
    this.valueSchemaID = valueSchemaID;
    this.rmdRecord = rmdRecord;
    this.rmdProtocolVersionID = rmdProtocolVersionID;
  }

  public GenericRecord getRmdRecord() {
    return rmdRecord;
  }

  public int getValueSchemaId() {
    return valueSchemaID;
  }

  public int getRmdProtocolVersionID() {
    return rmdProtocolVersionID;
  }
}
