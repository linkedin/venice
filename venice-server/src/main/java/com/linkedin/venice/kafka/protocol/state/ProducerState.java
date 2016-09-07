/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.kafka.protocol.state;

@SuppressWarnings("all")
/** A record containing the state pertaining to the data sent by one upstream producer into one partition. */
public class ProducerState extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ProducerState\",\"namespace\":\"com.linkedin.venice.kafka.protocol.state\",\"fields\":[{\"name\":\"segmentNumber\",\"type\":\"int\",\"doc\":\"The current segment number corresponds to the last (highest) segment number for which we have seen a StartOfSegment control message.\"},{\"name\":\"segmentStatus\",\"type\":\"int\",\"doc\":\"The status of the current segment: 0 => In progress, 1 => Received non-final EndOfSegment, 2 => Received final EndOfSegment.\"},{\"name\":\"messageSequenceNumber\",\"type\":\"int\",\"doc\":\"The current message sequence number, within the current segment, which we have seen for this partition/producer pair.\"},{\"name\":\"messageTimestamp\",\"type\":\"long\",\"doc\":\"The timestamp included in the last message we have seen for this partition/producer pair.\"},{\"name\":\"checksumType\",\"type\":\"int\",\"doc\":\"The current mapping is the following: 0 => None, 1 => MD5, 2 => Adler32, 3 => CRC32.\"},{\"name\":\"checksumValue\",\"type\":\"bytes\",\"doc\":\"The value of the checksum computed since the last StartOfSegment ControlMessage.\"},{\"name\":\"aggregates\",\"type\":{\"type\":\"map\",\"values\":\"long\"},\"doc\":\"The aggregates that have been computed so far since the last StartOfSegment ControlMessage.\"},{\"name\":\"debugInfo\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"doc\":\"The debug info received as part of the last StartOfSegment ControlMessage.\"}]}");
  /** The current segment number corresponds to the last (highest) segment number for which we have seen a StartOfSegment control message. */
  public int segmentNumber;
  /** The status of the current segment: 0 => In progress, 1 => Received non-final EndOfSegment, 2 => Received final EndOfSegment. */
  public int segmentStatus;
  /** The current message sequence number, within the current segment, which we have seen for this partition/producer pair. */
  public int messageSequenceNumber;
  /** The timestamp included in the last message we have seen for this partition/producer pair. */
  public long messageTimestamp;
  /** The current mapping is the following: 0 => None, 1 => MD5, 2 => Adler32, 3 => CRC32. */
  public int checksumType;
  /** The value of the checksum computed since the last StartOfSegment ControlMessage. */
  public java.nio.ByteBuffer checksumValue;
  /** The aggregates that have been computed so far since the last StartOfSegment ControlMessage. */
  public java.util.Map<java.lang.CharSequence,java.lang.Long> aggregates;
  /** The debug info received as part of the last StartOfSegment ControlMessage. */
  public java.util.Map<java.lang.CharSequence,java.lang.CharSequence> debugInfo;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return segmentNumber;
    case 1: return segmentStatus;
    case 2: return messageSequenceNumber;
    case 3: return messageTimestamp;
    case 4: return checksumType;
    case 5: return checksumValue;
    case 6: return aggregates;
    case 7: return debugInfo;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: segmentNumber = (java.lang.Integer)value$; break;
    case 1: segmentStatus = (java.lang.Integer)value$; break;
    case 2: messageSequenceNumber = (java.lang.Integer)value$; break;
    case 3: messageTimestamp = (java.lang.Long)value$; break;
    case 4: checksumType = (java.lang.Integer)value$; break;
    case 5: checksumValue = (java.nio.ByteBuffer)value$; break;
    case 6: aggregates = (java.util.Map<java.lang.CharSequence,java.lang.Long>)value$; break;
    case 7: debugInfo = (java.util.Map<java.lang.CharSequence,java.lang.CharSequence>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
