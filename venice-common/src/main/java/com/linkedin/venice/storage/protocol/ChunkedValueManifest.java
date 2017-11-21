/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.storage.protocol;

@SuppressWarnings("all")
/** This record maintains chunking information in order to re-assemble a value value that was split in many chunks. The version of this schema is intentionally set to -1 because this is what will be used in the schema part of the value field, representing a special system-type schema, as opposed to a user-defined schema. */
public class ChunkedValueManifest extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ChunkedValueManifest\",\"namespace\":\"com.linkedin.venice.storage.protocol\",\"fields\":[{\"name\":\"producerGUID\",\"type\":{\"type\":\"fixed\",\"name\":\"GUID\",\"namespace\":\"com.linkedin.venice.kafka.protocol\",\"size\":16},\"doc\":\"The GUID belonging to the producer of this value.\"},{\"name\":\"segmentNumber\",\"type\":\"int\",\"doc\":\"The segment number of the first chunk sent as part of this multi-chunk value.\"},{\"name\":\"messageSequenceNumber\",\"type\":\"int\",\"doc\":\"The sequence number of the first chunk sent as part of this multi-chunk value.\"},{\"name\":\"numberOfChunks\",\"type\":\"int\",\"doc\":\"The number of chunks this message was split into. The chunk indices will start from zero and go up to numberOfChunks - 1.\"},{\"name\":\"schemaId\",\"type\":\"int\",\"doc\":\"An identifier used to determine how the full value (after chunk re-assembly) can be deserialized. This is the ID of the user-defined schema.\"}]}");
  /** The GUID belonging to the producer of this value. */
  public com.linkedin.venice.kafka.protocol.GUID producerGUID;
  /** The segment number of the first chunk sent as part of this multi-chunk value. */
  public int segmentNumber;
  /** The sequence number of the first chunk sent as part of this multi-chunk value. */
  public int messageSequenceNumber;
  /** The number of chunks this message was split into. The chunk indices will start from zero and go up to numberOfChunks - 1. */
  public int numberOfChunks;
  /** An identifier used to determine how the full value (after chunk re-assembly) can be deserialized. This is the ID of the user-defined schema. */
  public int schemaId;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return producerGUID;
    case 1: return segmentNumber;
    case 2: return messageSequenceNumber;
    case 3: return numberOfChunks;
    case 4: return schemaId;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: producerGUID = (com.linkedin.venice.kafka.protocol.GUID)value$; break;
    case 1: segmentNumber = (java.lang.Integer)value$; break;
    case 2: messageSequenceNumber = (java.lang.Integer)value$; break;
    case 3: numberOfChunks = (java.lang.Integer)value$; break;
    case 4: schemaId = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
