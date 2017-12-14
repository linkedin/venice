/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.kafka.protocol.state;

@SuppressWarnings("all")
/** This record maintains store-version level state, such as the StartOfBufferReplay Control Message, in the case of Hybrid Stores. */
public class StoreVersionState extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"StoreVersionState\",\"namespace\":\"com.linkedin.venice.kafka.protocol.state\",\"fields\":[{\"name\":\"sorted\",\"type\":\"boolean\",\"doc\":\"Whether the messages inside the current store-version, between the 'StartOfPush' and 'EndOfPush' control messages, are lexicographically sorted by key bytes. N.B.: This field used to be stored in v2 of the PartitionState schema, but it has now been removed from there.\"},{\"name\":\"startOfBufferReplay\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"StartOfBufferReplay\",\"namespace\":\"com.linkedin.venice.kafka.protocol\",\"fields\":[{\"name\":\"sourceOffsets\",\"type\":{\"type\":\"array\",\"items\":\"long\"},\"doc\":\"Array of offsets from the real-time buffer topic at which the Buffer Replay Service started replaying data. The index position of the array corresponds to the partition number in the real-time buffer.\"},{\"name\":\"sourceKafkaCluster\",\"type\":\"string\",\"doc\":\"Kafka bootstrap servers URL of the cluster where the source buffer exists.\"},{\"name\":\"sourceTopicName\",\"type\":\"string\",\"doc\":\"Name of the source buffer topic.\"}]}],\"doc\":\"If a StartOfBufferReplay has been consumed, then it is stored in its entirety here, otherwise, this field is null.\"},{\"name\":\"chunked\",\"type\":\"boolean\",\"doc\":\"Whether the messages inside current store-version are encoded with chunking support. If true, this means keys will be prefixed with ChunkId, and values may contain a ChunkedValueManifest (if schema is defined as -1).\",\"default\":false},{\"name\":\"compressionStrategy\",\"type\":\"int\",\"doc\":\"What type of compression strategy the current push are used. Using int because Avro Enums are not evolvable. The mapping is the following: 0 => NO_OP, 1 => GZIP\",\"default\":0}]}");
  /** Whether the messages inside the current store-version, between the 'StartOfPush' and 'EndOfPush' control messages, are lexicographically sorted by key bytes. N.B.: This field used to be stored in v2 of the PartitionState schema, but it has now been removed from there. */
  public boolean sorted;
  /** If a StartOfBufferReplay has been consumed, then it is stored in its entirety here, otherwise, this field is null. */
  public com.linkedin.venice.kafka.protocol.StartOfBufferReplay startOfBufferReplay;
  /** Whether the messages inside current store-version are encoded with chunking support. If true, this means keys will be prefixed with ChunkId, and values may contain a ChunkedValueManifest (if schema is defined as -1). */
  public boolean chunked;
  /** What type of compression strategy the current push are used. Using int because Avro Enums are not evolvable. The mapping is the following: 0 => NO_OP, 1 => GZIP */
  public int compressionStrategy;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return sorted;
    case 1: return startOfBufferReplay;
    case 2: return chunked;
    case 3: return compressionStrategy;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: sorted = (java.lang.Boolean)value$; break;
    case 1: startOfBufferReplay = (com.linkedin.venice.kafka.protocol.StartOfBufferReplay)value$; break;
    case 2: chunked = (java.lang.Boolean)value$; break;
    case 3: compressionStrategy = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
