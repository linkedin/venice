/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.controller.kafka.protocol.admin;

@SuppressWarnings("all")
public class HybridStoreConfigRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"HybridStoreConfigRecord\",\"namespace\":\"com.linkedin.venice.controller.kafka.protocol.admin\",\"fields\":[{\"name\":\"rewindTimeInSeconds\",\"type\":\"long\"},{\"name\":\"offsetLagThresholdToGoOnline\",\"type\":\"long\"},{\"name\":\"producerTimestampLagThresholdToGoOnlineInSeconds\",\"type\":\"long\",\"default\":-1},{\"name\":\"dataReplicationPolicy\",\"type\":\"int\",\"doc\":\"Real-time Samza job data replication policy. Using int because Avro Enums are not evolvable 0 => NON_AGGREGATE, 1 => AGGREGATE, 2 => ACTIVE_ACTIVE\",\"default\":0},{\"name\":\"bufferReplayPolicy\",\"type\":\"int\",\"doc\":\"Policy that will be used during buffer replay. rewindTimeInSeconds defines the delta. 0 => REWIND_FROM_EOP (replay from 'EOP - rewindTimeInSeconds'), 1 => REWIND_FROM_SOP (replay from 'SOP - rewindTimeInSeconds')\",\"default\":0}]}");
  public long rewindTimeInSeconds;
  public long offsetLagThresholdToGoOnline;
  public long producerTimestampLagThresholdToGoOnlineInSeconds;
  /** Real-time Samza job data replication policy. Using int because Avro Enums are not evolvable 0 => NON_AGGREGATE, 1 => AGGREGATE, 2 => ACTIVE_ACTIVE */
  public int dataReplicationPolicy;
  /** Policy that will be used during buffer replay. rewindTimeInSeconds defines the delta. 0 => REWIND_FROM_EOP (replay from 'EOP - rewindTimeInSeconds'), 1 => REWIND_FROM_SOP (replay from 'SOP - rewindTimeInSeconds') */
  public int bufferReplayPolicy;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return rewindTimeInSeconds;
    case 1: return offsetLagThresholdToGoOnline;
    case 2: return producerTimestampLagThresholdToGoOnlineInSeconds;
    case 3: return dataReplicationPolicy;
    case 4: return bufferReplayPolicy;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: rewindTimeInSeconds = (java.lang.Long)value$; break;
    case 1: offsetLagThresholdToGoOnline = (java.lang.Long)value$; break;
    case 2: producerTimestampLagThresholdToGoOnlineInSeconds = (java.lang.Long)value$; break;
    case 3: dataReplicationPolicy = (java.lang.Integer)value$; break;
    case 4: bufferReplayPolicy = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
