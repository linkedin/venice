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
  private int valueSchemaId;
  private int rmdProtocolVersionId;
  private GenericRecord rmdRecord;

  private ChunkedValueManifest rmdManifest;

  public RmdWithValueSchemaId(
      int valueSchemaId,
      int rmdProtocolVersionId,
      GenericRecord rmdRecord,
      ChunkedValueManifest rmdManifest) {
    this.valueSchemaId = valueSchemaId;
    this.rmdRecord = rmdRecord;
    this.rmdProtocolVersionId = rmdProtocolVersionId;
    this.rmdManifest = rmdManifest;
  }

  public RmdWithValueSchemaId(int valueSchemaId, int rmdProtocolVersionId, GenericRecord rmdRecord) {
    this.valueSchemaId = valueSchemaId;
    this.rmdRecord = rmdRecord;
    this.rmdProtocolVersionId = rmdProtocolVersionId;
  }

  public RmdWithValueSchemaId() {
  }

  public void setValueSchemaId(int valueSchemaId) {
    this.valueSchemaId = valueSchemaId;
  }

  public void setRmdProtocolVersionId(int rmdProtocolVersionId) {
    this.rmdProtocolVersionId = rmdProtocolVersionId;
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
    return valueSchemaId;
  }

  public int getRmdProtocolVersionId() {
    return rmdProtocolVersionId;
  }

  public ChunkedValueManifest getRmdManifest() {
    return rmdManifest;
  }
}
