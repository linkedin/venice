package com.linkedin.venice.message;

/**
 * Created by clfung on 9/11/14.
 */
public class VeniceMessage {

  // TODO: eliminate magic numbers when finished debugging
  public static final int DEFAULT_MAGIC_BYTE = 13;
  public static final int DEFAULT_SCHEMA_VERSION = 17;

  private byte magicByte;
  private byte schemaVersion;

  private OperationType operationType;
  private String payload;

  // TODO: find best data type for timestamp
  private Object timestamp;

  public VeniceMessage(OperationType type, String payload) {

    magicByte = DEFAULT_MAGIC_BYTE;
    schemaVersion = DEFAULT_SCHEMA_VERSION;

    operationType = type;
    this.payload = payload;

    timestamp = null;

  }

  public byte getMagicByte() {
    return magicByte;
  }

  public OperationType getOperationType() {
    return operationType;
  }

  public byte getSchemaVersion() {
    return schemaVersion;
  }

  public String getPayload() {
    return payload;
  }

  public String toString() {
    return operationType.toString() + " " + payload;
  }

  public Object getTimestamp() { return timestamp; }

}
