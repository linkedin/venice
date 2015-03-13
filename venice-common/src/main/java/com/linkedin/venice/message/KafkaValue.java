package com.linkedin.venice.message;

import com.linkedin.venice.exceptions.VeniceMessageException;

/**
 * Class which stores the components of VeniceMessage, and is the format specified in the Kafka Serializer
 */
public class KafkaValue {

  // TODO: eliminate magic numbers when finished debugging
  public static final byte DEFAULT_MAGIC_BYTE = 22;
  public static final byte DEFAULT_SCHEMA_ID = 17;
  public static final byte ZONE_ID = 0; //TODO hard coded for now. Later need to get rid of this.

  private byte magicByte;
  private short schemaVersionId;

  private OperationType operationType;
  private long timestamp;
  private byte zoneId;
  private byte[] payload;

  // A message without a payload (used for non put operations)
  public KafkaValue(OperationType type) {
    this.magicByte = DEFAULT_MAGIC_BYTE;
    this.schemaVersionId = DEFAULT_SCHEMA_ID;
    if (type == OperationType.PUT || type == OperationType.PARTIAL_PUT) {
      throw new VeniceMessageException("Operation type " + type + " is invalid for Venice message without payload.");
    }
    this.operationType = type;
    this.timestamp = System.currentTimeMillis();
    this.zoneId = ZONE_ID;
    this.payload = new byte[0];
  }

  public KafkaValue(OperationType type, byte[] value, short schemaId) {
    this.magicByte = DEFAULT_MAGIC_BYTE;
    this.schemaVersionId = schemaId;
    this.operationType = type;
    this.timestamp = System.currentTimeMillis();
    this.zoneId = ZONE_ID;
    this.payload = value;
  }

  public KafkaValue(OperationType type, byte[] value) {
    this(type, value, DEFAULT_SCHEMA_ID);
  }

  public byte getMagicByte() {
    return magicByte;
  }

  public OperationType getOperationType() {
    return operationType;
  }

  public short getSchemaVersionId() {
    return schemaVersionId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public byte getZoneId() {
    return zoneId;
  }

  public byte[] getValue() {
    return payload;
  }

  public String toString() {
    return operationType.toString() + " schema-id:" + schemaVersionId + " time-stamp:" + timestamp
      + " zone-id:" + zoneId + " value:" + payload.toString();
  }
}
