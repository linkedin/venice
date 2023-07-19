package com.linkedin.davinci.replication;

import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import org.apache.avro.generic.GenericRecord;


/**
 * A POJO class that contains 3 things/fields:
 *    1. RMD record.
 *    2. RMD protocol version ID.
 *    3. Value schema ID used to generate the RMD schema.
 */
public class RmdWithValueSchemaId {
  private int valueSchemaID;
  private int rmdProtocolVersionID;
  private GenericRecord rmdRecord;

  private ChunkedValueManifest rmdManifest;

  public RmdWithValueSchemaId(
      int valueSchemaID,
      int rmdProtocolVersionID,
      GenericRecord rmdRecord,
      ChunkedValueManifest rmdManifest) {
    this.valueSchemaID = valueSchemaID;
    this.rmdRecord = rmdRecord;
    this.rmdProtocolVersionID = rmdProtocolVersionID;
    this.rmdManifest = rmdManifest;
  }

  public RmdWithValueSchemaId(int valueSchemaID, int rmdProtocolVersionID, GenericRecord rmdRecord) {
    this.valueSchemaID = valueSchemaID;
    this.rmdRecord = rmdRecord;
    this.rmdProtocolVersionID = rmdProtocolVersionID;
  }

  public RmdWithValueSchemaId() {
  }

  public void setValueSchemaID(int valueSchemaID) {
    this.valueSchemaID = valueSchemaID;
  }

  public void setRmdProtocolVersionID(int rmdProtocolVersionID) {
    this.rmdProtocolVersionID = rmdProtocolVersionID;
  }

  public void setRmdRecord(GenericRecord rmdRecord) {
    this.rmdRecord = rmdRecord;
  }

  public void setRmdManifest(ChunkedValueManifest rmdManifest) {
    this.rmdManifest = rmdManifest;
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

  public ChunkedValueManifest getRmdManifest() {
    return rmdManifest;
  }
}
