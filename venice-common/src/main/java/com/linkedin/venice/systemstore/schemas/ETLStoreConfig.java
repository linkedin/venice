/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.systemstore.schemas;

@SuppressWarnings("all")
public class ETLStoreConfig extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ETLStoreConfig\",\"namespace\":\"com.linkedin.venice.systemstore.schemas\",\"fields\":[{\"name\":\"etledUserProxyAccount\",\"type\":\"string\"},{\"name\":\"regularVersionETLEnabled\",\"type\":\"boolean\"},{\"name\":\"futureVersionETLEnabled\",\"type\":\"boolean\"}]}");
  public java.lang.CharSequence etledUserProxyAccount;
  public boolean regularVersionETLEnabled;
  public boolean futureVersionETLEnabled;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return etledUserProxyAccount;
    case 1: return regularVersionETLEnabled;
    case 2: return futureVersionETLEnabled;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: etledUserProxyAccount = (java.lang.CharSequence)value$; break;
    case 1: regularVersionETLEnabled = (java.lang.Boolean)value$; break;
    case 2: futureVersionETLEnabled = (java.lang.Boolean)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
