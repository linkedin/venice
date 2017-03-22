/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.controller.kafka.protocol.admin;

@SuppressWarnings("all")
public class SetStoreCurrentVersion extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"SetStoreCurrentVersion\",\"namespace\":\"com.linkedin.venice.controller.kafka.protocol.admin\",\"fields\":[{\"name\":\"clusterName\",\"type\":\"string\"},{\"name\":\"storeName\",\"type\":\"string\"},{\"name\":\"currentVersion\",\"type\":\"int\"}]}");
  public java.lang.CharSequence clusterName;
  public java.lang.CharSequence storeName;
  public int currentVersion;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return clusterName;
    case 1: return storeName;
    case 2: return currentVersion;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: clusterName = (java.lang.CharSequence)value$; break;
    case 1: storeName = (java.lang.CharSequence)value$; break;
    case 2: currentVersion = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
