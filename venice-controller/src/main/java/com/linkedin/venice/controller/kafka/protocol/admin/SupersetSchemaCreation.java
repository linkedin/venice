/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.controller.kafka.protocol.admin;

@SuppressWarnings("all")
public class SupersetSchemaCreation extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"SupersetSchemaCreation\",\"namespace\":\"com.linkedin.venice.controller.kafka.protocol.admin\",\"fields\":[{\"name\":\"clusterName\",\"type\":\"string\"},{\"name\":\"storeName\",\"type\":\"string\"},{\"name\":\"valueSchema\",\"type\":{\"type\":\"record\",\"name\":\"SchemaMeta\",\"fields\":[{\"name\":\"schemaType\",\"type\":\"int\",\"doc\":\"0 => Avro-1.4, and we can add more if necessary\"},{\"name\":\"definition\",\"type\":\"string\"}]}},{\"name\":\"valueSchemaId\",\"type\":\"int\"},{\"name\":\"supersetSchema\",\"type\":\"SchemaMeta\"},{\"name\":\"supersetSchemaId\",\"type\":\"int\"}]}");
  public java.lang.CharSequence clusterName;
  public java.lang.CharSequence storeName;
  public com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta valueSchema;
  public int valueSchemaId;
  public com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta supersetSchema;
  public int supersetSchemaId;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return clusterName;
    case 1: return storeName;
    case 2: return valueSchema;
    case 3: return valueSchemaId;
    case 4: return supersetSchema;
    case 5: return supersetSchemaId;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: clusterName = (java.lang.CharSequence)value$; break;
    case 1: storeName = (java.lang.CharSequence)value$; break;
    case 2: valueSchema = (com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta)value$; break;
    case 3: valueSchemaId = (java.lang.Integer)value$; break;
    case 4: supersetSchema = (com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta)value$; break;
    case 5: supersetSchemaId = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
